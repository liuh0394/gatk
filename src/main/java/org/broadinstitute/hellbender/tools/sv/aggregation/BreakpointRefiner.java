package org.broadinstitute.hellbender.tools.sv.aggregation;

import com.google.common.collect.Sets;
import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.StructuralVariantType;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVUtils;
import org.broadinstitute.hellbender.tools.sv.SVCallRecord;
import org.broadinstitute.hellbender.tools.sv.SVCallRecordUtils;
import org.broadinstitute.hellbender.tools.sv.SplitReadEvidence;
import org.broadinstitute.hellbender.utils.Utils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Refines variant breakpoints using split read evidence.
 *
 * The start and end of the breakpoint are tested independently. At each end we perform a series of Poisson
 * tests using the following model:
 *
 *  Given:
 *      d_i : depth of sample i
 *      c_i : split read count of sample i
 *
 *   Define a Poisson model of split read counts:
 *      r_i = c_i / d_i : normalized split read count of sample i
 *      m_c : median carrier r_i
 *      m_b : median background (non-carrier) r_i
 *      mu : mean depth of all samples
 *
 *      lambda = mu * m_c : expected carrier count
 *      X ~ Poisson(lambda) : carrier count model
 *      x_b = round(mu * m_b) : adjusted median background count
 *
 *   Calculate probability of observing the background count:
 *      p = P(X < x_b)
 *
 *   We then select the site with the lowest score. Breakpoint end positions are restricted by a lowerbound that depends
 *   on the refined start position (see {@link BreakpointRefiner#getEndLowerBound(SVCallRecord, int)}.
 */
public class BreakpointRefiner {

    private final Map<String,Double> sampleCoverageMap;
    private final SAMSequenceDictionary dictionary;
    /**
     * Number bases that insertion split read positions can pass by the original breakpoint, when left-clipped
     * reads are to the left of the breakpoint and right-clipped reads to the right.
     */
    protected int maxInsertionSplitReadCrossDistance;

    public static final int DEFAULT_MAX_INSERTION_CROSS_DISTANCE = 20;
    public static final byte MAX_SR_QUALITY = 99;

    /**
     * @param sampleCoverageMap map with (sample id, per-base sample coverage) entries
     * @param dictionary reference dictionary
     */
    public BreakpointRefiner(final Map<String, Double> sampleCoverageMap, int maxInsertionSplitReadCrossDistance,
                             final SAMSequenceDictionary dictionary) {
        this.sampleCoverageMap = Utils.nonNull(sampleCoverageMap);
        this.dictionary = Utils.nonNull(dictionary);
        this.maxInsertionSplitReadCrossDistance = maxInsertionSplitReadCrossDistance;
    }

    /**
     * Performs refinement on one side of a breakpoint
     *
     * @param sortedEvidence split read evidence to test, sorted by position
     * @param carrierSamples carrier sample ids
     * @param backgroundSamples background sample ids
     * @param defaultPosition position to use if test cannot be performed (no evidence or carriers)
     * @return pair containing site with refined breakpoints and probability (null if no evidence or carriers)
     */
    protected static SplitReadSite refineSplitReadSite(final List<SplitReadEvidence> sortedEvidence,
                                                       final Collection<String> carrierSamples,
                                                       final Collection<String> backgroundSamples,
                                                       final Map<String, Double> sampleCoverageMap,
                                                       final int representativeDepth,
                                                       final int defaultPosition) {
        Utils.validateArg(sampleCoverageMap.keySet().containsAll(carrierSamples),
                "One or more carrier samples not found in sample coverage map");
        Utils.validateArg(sampleCoverageMap.keySet().containsAll(backgroundSamples),
                "One or more non-carrier samples not found in sample coverage map");

        // Default case
        if (sortedEvidence.isEmpty() || carrierSamples.isEmpty()) {
            return new SplitReadSite(defaultPosition, Collections.emptyMap(), null);
        }

        EvidenceStatUtils.PoissonTestResult minPResult = null;
        Integer minDistance = null;
        Integer minPPosition = null;
        Map<String, Integer> minPSampleCounts = null;
        int position;
        Map<String, Integer> sampleCounts = new HashMap<>();
        for (int i = 0; i < sortedEvidence.size(); i++) {
            final SplitReadEvidence e = sortedEvidence.get(i);
            position = e.getStart();
            sampleCounts.put(e.getSample(), e.getCount());
            if (i == sortedEvidence.size() - 1 || sortedEvidence.get(i + 1).getStart() != position) {
                final EvidenceStatUtils.PoissonTestResult result = EvidenceStatUtils.calculateOneSamplePoissonTest(
                        sampleCounts, carrierSamples, backgroundSamples, sampleCoverageMap, representativeDepth
                );
                final int dist = Math.abs(position - defaultPosition);
                if (minPResult == null || result.getP() < minPResult.getP() || (result.getP() == minPResult.getP() && dist < minDistance)) {
                    minPResult = result;
                    minPPosition = position;
                    minPSampleCounts = sampleCounts;
                    minDistance = dist;
                }
                sampleCounts = new HashMap<>();
            }
        }
        return new SplitReadSite(minPPosition, minPSampleCounts, minPResult);
    }

    /**
     * Performs breakend refinement for a call
     *
     * @param record with split read evidence
     * @return record with new breakpoints
     */
    public SVCallRecord refineCall(final SVCallRecord record,
                                   final List<SplitReadEvidence> startEvidence,
                                   final List<SplitReadEvidence> endEvidence,
                                   final Set<String> excludedSamples) {
        Utils.nonNull(record);
        SVCallRecordUtils.validateCoordinatesWithDictionary(record, dictionary);

        // Sample sets
        final Set<String> includedSamples = Sets.difference(record.getAllSamples(), excludedSamples);
        final Set<String> carrierSamples = record.getCarrierSampleSet();
        final Set<String> includedCarrierSamples = Sets.difference(carrierSamples, excludedSamples);
        final Set<String> includedBackgroundSamples = Sets.difference(includedSamples, includedCarrierSamples);
        final int representativeDepth = EvidenceStatUtils.computeRepresentativeDepth(sampleCoverageMap.values());

        // Refine start
        final SplitReadSite refinedStartSite = refineSplitReadSite(startEvidence, includedCarrierSamples,
                includedBackgroundSamples, sampleCoverageMap, representativeDepth, record.getPositionA());

        // Refine end
        final int endLowerBound = getEndLowerBound(record, refinedStartSite.getPosition());
        final int defaultEndPosition = Math.max(endLowerBound, record.getPositionB());
        final List<SplitReadEvidence> validEndEvidence = getValidEndSplitReadSites(endEvidence, endLowerBound);
        final SplitReadSite refinedEndSite = refineSplitReadSite(validEndEvidence, includedCarrierSamples,
                includedBackgroundSamples, sampleCoverageMap, representativeDepth, defaultEndPosition);

        // Compute stats on sum of start and end counts
        final EvidenceStatUtils.PoissonTestResult bothsideResult = calculateBothsideTest(refinedStartSite, refinedEndSite,
                includedSamples, includedCarrierSamples, includedBackgroundSamples, sampleCoverageMap, representativeDepth);

        final int refinedStartPosition = refinedStartSite.getPosition();
        final int refinedEndPosition = refinedEndSite.getPosition();
        final Integer length = record.getType().equals(StructuralVariantType.INS) ? record.getLength() : null;

        final Integer startQuality = Double.isNaN(refinedStartSite.getP()) ? null : EvidenceStatUtils.probToQual(refinedStartSite.getP(), MAX_SR_QUALITY);
        final Integer endQuality = Double.isNaN(refinedEndSite.getP()) ? null : EvidenceStatUtils.probToQual(refinedEndSite.getP(), MAX_SR_QUALITY);
        final Integer totalQuality = Double.isNaN(bothsideResult.getP()) ? null : EvidenceStatUtils.probToQual(bothsideResult.getP(), MAX_SR_QUALITY);
        final Map<String, Object> refinedAttr = new HashMap<>(record.getAttributes());
        refinedAttr.put(GATKSVVCFConstants.START_SPLIT_QUALITY_ATTRIBUTE, startQuality);
        refinedAttr.put(GATKSVVCFConstants.END_SPLIT_QUALITY_ATTRIBUTE, endQuality);
        refinedAttr.put(GATKSVVCFConstants.TOTAL_SPLIT_QUALITY_ATTRIBUTE, totalQuality);

        final Integer startCarrierSignal = EvidenceStatUtils.carrierSignalFraction(refinedStartSite.getCarrierSignal(),
                refinedStartSite.getBackgroundSignal());
        final Integer endCarrierSignal = EvidenceStatUtils.carrierSignalFraction(refinedEndSite.getCarrierSignal(),
                refinedEndSite.getBackgroundSignal());
        final Integer totalCarrierSignal = EvidenceStatUtils.carrierSignalFraction(bothsideResult.getCarrierSignal(),
                bothsideResult.getBackgroundSignal());
        refinedAttr.put(GATKSVVCFConstants.START_SPLIT_CARRIER_SIGNAL_ATTRIBUTE, startCarrierSignal);
        refinedAttr.put(GATKSVVCFConstants.END_SPLIT_CARRIER_SIGNAL_ATTRIBUTE, endCarrierSignal);
        refinedAttr.put(GATKSVVCFConstants.TOTAL_SPLIT_CARRIER_SIGNAL_ATTRIBUTE, totalCarrierSignal);

        final List<Genotype> genotypes = record.getGenotypes();
        final GenotypesContext newGenotypes = GenotypesContext.create(genotypes.size());
        for (final Genotype genotype : genotypes) {
            final String sample = genotype.getSampleName();
            final GenotypeBuilder genotypeBuilder = new GenotypeBuilder(genotype);
            genotypeBuilder.attribute(GATKSVVCFConstants.START_SPLIT_READ_COUNT_ATTRIBUTE, refinedStartSite.getCount(sample));
            genotypeBuilder.attribute(GATKSVVCFConstants.END_SPLIT_READ_COUNT_ATTRIBUTE, refinedEndSite.getCount(sample));
            newGenotypes.add(genotypeBuilder.make());
        }

        // Create new record
        return new SVCallRecord(record.getId(), record.getContigA(), refinedStartPosition,
                record.getStrandA(), record.getContigB(), refinedEndPosition, record.getStrandB(), record.getType(),
                length, record.getAlgorithms(), record.getAlleles(), newGenotypes, refinedAttr);
    }

    private static EvidenceStatUtils.PoissonTestResult calculateBothsideTest(final SplitReadSite startSite,
                                                                             final SplitReadSite endSite,
                                                                             final Set<String> calledSamples,
                                                                             final Collection<String> carrierSamples,
                                                                             final Collection<String> backgroundSamples,
                                                                             final Map<String, Double> sampleCoverageMap,
                                                                             final double representativeDepth) {
        final Map<String, Integer> sampleCountSums = new HashMap<>(SVUtils.hashMapCapacity(calledSamples.size()));
        for (final String sample : calledSamples) {
            sampleCountSums.put(sample, startSite.getCount(sample) + endSite.getCount(sample));
        }
        return EvidenceStatUtils.calculateOneSamplePoissonTest(sampleCountSums,
               carrierSamples, backgroundSamples, sampleCoverageMap, representativeDepth);
    }

    /**
     * Filters end sites with position less than lower-bound
     *
     * @param endEvidence
     * @param lowerBound min position
     * @return filtered set of end sites
     */
    private static List<SplitReadEvidence> getValidEndSplitReadSites(final List<SplitReadEvidence> endEvidence, final int lowerBound) {
        return endEvidence.stream().filter(s -> s.getStart() >= lowerBound).collect(Collectors.toList());
    }

    /**
     * Determines lower-bound on end site position (inclusive). For inter-chromosomal variants, boundaries are at the
     * start of the chromsome (any position is valid). For insertions, {@link BreakpointRefiner#maxInsertionSplitReadCrossDistance}
     * is used to determine how far past the original breakpoint it can be. Otherwise, we just use the new start position.
     *
     * @param call
     * @param refinedStartPosition new start position of call
     * @return position
     */
    private int getEndLowerBound(final SVCallRecord call, final int refinedStartPosition) {
        if (!call.isIntrachromosomal()) {
            return 1;
        }
        if (call.getType().equals(StructuralVariantType.INS)) {
            return refinedStartPosition - maxInsertionSplitReadCrossDistance;
        }
        return refinedStartPosition + 1;
    }
}
