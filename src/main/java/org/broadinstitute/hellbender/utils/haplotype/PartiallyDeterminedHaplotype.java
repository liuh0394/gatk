package org.broadinstitute.hellbender.utils.haplotype;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.servo.util.Objects;
import htsjdk.samtools.Cigar;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class for holding the PartiallyDeterminedHaplotype constructs that are needed for the PDHMM.
 *
 * The primary function of a "partiallyDeterminedHaplotype" is so that the PDHMM can compress the computation of Haplotype
 * score max's that gets marginalized in the genotyper down into a single HMM operation. You can think of each PDHMM haplotype
 * as being representative of a single allele in the genotyper (be that Ref or Alt) where every other discovered variant in the
 * entire region is "undetermined." An undetermined SNP might mean that either the REF or ALT bases at that site are treated
 * as matches within the HMM model. An undetermined INDEL is slightly more complicated, if its an undetermined deletion then
 * we expect the haplotype bases to match the reference and the mismatches to be recorded in the jump array. If the
 * undetermined event is an Insertion then we "flip" the insertion by adding the inserted bases to the reference haplotype
 * and treating it as undetermined for both.
 *
 * Differences of note from Haplotype:
 *  - alternateBases array - This is a hap-length array that stores summaries of the undetermined bases. For bases that don't
 *                           have any undetermined behavior the byte should simply be 0. Otherwise the bytes bear a representation
 *                           of the event as a bitset (with the keys for what each bit represents at the top of this class)
 *  - determinedPosition/alleleBearingVariantContext - Since we construct the PDHaps with only one event "determined" this position
 *                                                     and variant contexts store exactly WHAT that event is for the haplotype as
 *                                                     well as what position it counts as for various bookkeping tasks.
 *                                               NOTE: these are used for EventMap construction since for PDHaplotypes the
 *                                                     event map should contain ONLY this event.
 *  - isDeterminedAlleleRef - This variant simply accounts for whether or not this is a reference determined allele to be
 *                            assigned to reference when genotyping later on.
 *  - constituentBuiltEvents - Since this object is artifically constructed form a set of constituent events we may as well
 *                             store them here for debugging purposes.
 *
 * Known issues/limitations:
 *  - Despite having been assured by Illumina engineers that they do NOT allow undetermined SNPs to be in the middle of
 *    indels. I have found at least one edge case involving single base deletions competing with snps at the same base where
 *    DRAGEN 3.7.8 evidently breaks this rule. I'm not convinced this is terribly improtant to FE but it should be noted.
 */
public final class  PartiallyDeterminedHaplotype extends Haplotype {
    private static final long serialVersionUID = 1L;

    //COMMENTS DIRECTLY FROM DRAGEN CODE:
    // This is the array which gives information if the base in the haplotype is
    // resolved or not.
    // 8-bit field
    //  0   0   0   0   0   0   0   0
    // SNP  DELETE  A   C   G   T   N
    // If SNP is true, then bits 3-7 are active indicating which bases are also possible
    // Bits 1-2 indicate delete states
    // 01 - Delete region start
    // 10 - Delete region stops
    // 11 - Delete has length 1, start and stop position is the same
    // All 8 bits 0 indicate either a resolved position or if w
    final public static byte SNP = 1;
    final public static byte DEL_START = 2;
    final public static byte DEL_END = 4;
    final public static byte A = 8;
    final public static byte C = 16;
    final public static byte G = 32;
    final public static byte T = 64;
    final public static byte N = (byte) 128;

    public byte[] getAlternateBases() {
        return alternateBases;
    }

    private final byte[] alternateBases;
    private final List<VariantContext> constituentBuiltEvents;
    // NOTE: we must store ALL of the determined events at this site (which is different than the constituent events, we expect the constituent
    //       events for one of these objects to only be a single element) for the purposes of the overlapping reads PDHMM optimization.
    //       At Multiallelic sites, we ultimately genotype all of the alleles at once. If we aren't careful, reads that only overlap some
    //       alleles at a site will end up with incorrect/undetermined PDHMM scores for a subset of alleles in the genotyper which can
    //       lead to false positives/poorly called sites.
    private List<VariantContext> allDeterminedEventsAtThisSite;

    //TODO Eventually these will have to be refactored to support multiple determined alleles per PDHaplotype
    private final VariantContext alleleBearingVariantContext; // NOTE this must be in both of the previous lists
    private final long determinedPosition;
    private final boolean isDeterminedAlleleRef;

    private SimpleInterval cachedExtent;

    /**
     * Compares two haplotypes first by their lengths and then by lexicographic order of their bases.
     */
    public static final Comparator<Haplotype> SIZE_AND_BASE_ORDER =
            Comparator.comparingInt((Haplotype hap) -> hap.getBases().length)
                    .thenComparing(Allele::getBaseString);

    /**
     * @param base                          base (reference) haplotype used to construct the PDHaplotype
     * @param isRefAllele                   is the determined allele reference or alt
     * @param pdBytes                       array of bytes indicating what bases are skips for the pdhmm
     * @param constituentEvents             events (both determined and undetermined) covered by this haplotype, should follow the rules for PD variants
     * @param eventWithVariant              event from @param constituentEvents that is determined for this pd haplotype
     * @param cigar                         haplotype cigar agianst the reference
     * @param determinedPosition            position (wrt the reference contig) that the haplotype should be considered determined //TODO this will be refactored to be a range of events in JointDetection
     * @param getAlignmentStartHapwrtRef    alignment startHapwrtRef from baseHaplotype corresponding to the in-memory storage of reference bases (must be set for trimming/clipping ops to work)
     */
    public PartiallyDeterminedHaplotype(final Haplotype base, boolean isRefAllele, byte[] pdBytes, List<VariantContext> constituentEvents,
                                        VariantContext eventWithVariant, Cigar cigar, long determinedPosition, int getAlignmentStartHapwrtRef) {
        super(base.getBases(), false, base.getAlignmentStartHapwrtRef(), cigar);
        this.setGenomeLocation(base.getGenomeLocation());
        this.alternateBases = pdBytes;
        this.constituentBuiltEvents = constituentEvents;
        this.alleleBearingVariantContext = eventWithVariant;
        this.allDeterminedEventsAtThisSite = Collections.singletonList(eventWithVariant);
        this.determinedPosition = determinedPosition;
        this.isDeterminedAlleleRef = isRefAllele;
        setAlignmentStartHapwrtRef(getAlignmentStartHapwrtRef);
    }

    @Override
    public byte[] getDisplayBases() {
        byte[] bytes = getBases().clone();
        for (int i = 0; i < bytes.length; i++) {
            if ((alternateBases[i] & ~7) != 0) { // Change the display byte if we have a partially determined SNP underneath it
                bytes[i] = 'P';
            }
        }
        return bytes;
    }

    @Override
    public String toString() {
        String output = "HapLen:"+length() +", "+new String(getDisplayBases());
        output = output + "\nUnresolved Bases["+alternateBases.length+"] "+Arrays.toString(alternateBases);
        return output + "\n"+getCigar().toString()+" "+ constituentBuiltEvents.stream()
                .map(v ->(v==this.alleleBearingVariantContext ?"*":"")+getDRAGENDebugVariantContextString((int)getStartPosition()).apply(v) )
                .collect(Collectors.joining("->"));
    }

    /**
     * A printout of the PD haplotype meant to be readable and easily comparable to the DRAGEN debug mode output for the same haplotype.
     */
    public static Function<VariantContext, String> getDRAGENDebugVariantContextString(final int offset) {
        return v -> "(" + Integer.toString(v.getStart() - offset + (v.isSimpleDeletion()? 1 : 0))+ (v.isSimpleInsertion()?".5":"") + ",Rlen=" + v.getLengthOnReference()+"," + v.getAlternateAlleles() + ")";
    }

    @Override
    public boolean equals( final Object h ) {
        return h instanceof PartiallyDeterminedHaplotype
                && getUniquenessValue() == ((Haplotype) h).getUniquenessValue()
                && isReference() == ((Haplotype) h).isReference()
                && determinedPosition == ((PartiallyDeterminedHaplotype) h).determinedPosition // (even if the basees exactly match we still want to be cautious)
                && Arrays.equals(getBases(), ((Haplotype) h).getBases())
                && Arrays.equals(alternateBases, ((PartiallyDeterminedHaplotype) h).alternateBases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(getBases()),Arrays.hashCode(alternateBases));
    }

    public List<VariantContext> getDeterminedAlleles() {
        return isDeterminedAlleleRef ? Collections.emptyList() : Collections.singletonList(alleleBearingVariantContext);
    }

    public void setAllDeterminedEventsAtThisSite(List<VariantContext> allDeterminedEventsAtThisSite) {
        this.allDeterminedEventsAtThisSite = allDeterminedEventsAtThisSite;
        cachedExtent = null;
    }

    //NOTE: we never want the genotyper to handle reads that were not HMM scored, caching this extent helps keep us safe from messy sites
    public SimpleInterval getMaximumExtentOfSiteDeterminedAlleles() {
        if (cachedExtent == null) {
            cachedExtent = new SimpleInterval(alleleBearingVariantContext);
            for( VariantContext variantContext : allDeterminedEventsAtThisSite) {
                cachedExtent = cachedExtent.mergeWithContiguous(variantContext);
            }
        }
        return cachedExtent;
    }

    public long getDeterminedPosition() {
        return determinedPosition;
    }


    /**
     * A helper method for computing what the alternative bases array should look like for a particular set of alleles.
     */
    @VisibleForTesting
    public static byte[] getPDBytesForHaplotypes(final Allele refAllele, final Allele altAllele) {
        // Asserting we are either indel OR SNP
        byte[] output = new byte[altAllele.length() == refAllele.length() ? refAllele.length() : refAllele.length()-1 ];
        // SNP case
        if (altAllele.length() == refAllele.length()){
            output[0] += SNP;
            switch (altAllele.getBases()[0]) {
                case 'A':
                    output[0] += A;
                    break;
                case 'C':
                    output[0] += C;
                    break;
                case 'T':
                    output[0] += T;
                    break;
                case 'G':
                    output[0] += G;
                    break;
                default:
                    throw new RuntimeException("Found unexpected base in alt alleles");
            }

            // DEL case
        } else {
            output[0] += DEL_START;
            output[output.length - 1] += DEL_END; //TODO guardrail this
        }
        return output;
    }

    @Override
    public boolean isPartiallyDetermined() { return true; }

}
