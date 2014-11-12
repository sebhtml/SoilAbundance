/*
 *
 *
 *
 */
package gov.pnnl.marker.genes.common;

public class AccumuloStructure {

    public static final String SPSEP = "__SPSEP__";
    public static final String NLSEP = "__NLSEP__";
    public static final int    AA_MD5_OFFSET   =  0;    //  Protein MD5 offset in CSV file
    public static final int    SEQ_NAME_OFFSET =  3;    //  Sequence Name offset in CSV file
    public static final int    SCORE_OFFSET    =  5;    //  HMM bit score offset in CSV file
    public static final int    PP_OFFSET       = 11;    //  Posterior Probability offset in CSV file
    public static final int    PCTCOV_OFFSET   = 19;    //  Percent Coverage offset in CSV file
}

/*
 * $ACCUMULO_HOME/bin/tool.sh ~/darren/marker-genes-0.0.1-SNAPSHOT.jar gov.pnnl.marker.genes.ingest.MapReduceCSV -u root -p secret -z accum2 -i accumulo -t soilabundance --input users/darren/soilabundance.csv
 * 
 * 
 * ROW_ID
 * COLUMN_FAMILY - HMM_DOMAIN_INFO
 * HMM_DOMAIN_INFO_QUALIFIER - QUERY_NAME
 * HMM_DOMAIN_INFO_QUALIFIER - HMM_LENGTH
 * HMM_DOMAIN_INFO_QUALIFIER - SEQ_NAME
 * HMM_DOMAIN_INFO_QUALIFIER - DOMAIN_i_evalue
 * HMM_DOMAIN_INFO_QUALIFIER - DOMAIN_bitscore
 * HMM_DOMAIN_INFO_QUALIFIER - DOMAIN_bias
 * HMM_DOMAIN_INFO_QUALIFIER - HMM_from
 * HMM_DOMAIN_INFO_QUALIFIER - HMM_to
 * HMM_DOMAIN_INFO_QUALIFIER - QUERY_from
 * HMM_DOMAIN_INFO_QUALIFIER - QUERY_to
 * HMM_DOMAIN_INFO_QUALIFIER - POSTERIOR_PROBABILITY
 * 
 * COLUMN_FAMILY - HMM_METADATA
 * 
 * HMM_METADATA_QUALIFIER - DESCRIPTION
 * HMM_METADATA_QUALIFIER - FAMILY_TYPE
 * HMM_METADATA_QUALIFIER - GENE
 * HMM_METADATA_QUALIFIER - ENZYME_COMMISSION
 * 
 * COLUMN_FAMILY - HMM_CUTOFFS
 * 
 * HMM_CUTOFFS_QUALIFIER - DOMAIN_GA
 * HMM_CUTOFFS_QUALIFIER - DOMAIN_TC
 * HMM_CUTOFFS_QUALIFIER - DOMAIN_NC
 * 
 * VALUE
 * 
 * DNA_SEQUENCE_OF_HMM_DOMAIN_INFO_QUALIFIER_FOR_SEQ_NAME
 */