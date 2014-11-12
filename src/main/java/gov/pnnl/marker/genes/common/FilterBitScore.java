/*
 *
 *
 *
 */
package gov.pnnl.marker.genes.common;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

public class FilterBitScore extends WholeRowIterator {

    private Double score           = null;

    private Double postProb        = null;

    private Double percentCoverage = null;

    @Override
    public void init(final SortedKeyValueIterator<Key, Value> source, final Map<String, String> options, final IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        if (options.containsKey("score")) {
            score = Double.parseDouble(options.get("score"));
        }
        if (options.containsKey("post_prob")) {
            postProb = Double.parseDouble(options.get("post_prob"));
        }
        if (options.containsKey("percent_coverage")) {
            percentCoverage = Double.parseDouble(options.get("percent_coverage"));
        }
    }

    @Override
    public boolean filter(Text currentRow, List<Key> keys, List<Value> values) {
        for (int i = 0; i < keys.size(); ++i) {
            Key k = keys.get(i);
            Value v = values.get(i);
            if ((score != null) && k.getColumnQualifier().toString().equals("score")) {
                final Double value = Double.parseDouble(v.toString());
                if (value < score) {
                    return false;
                }
            }
            if ((postProb != null) && k.getColumnQualifier().toString().equals("post_prob")) {
                final Double value = Double.parseDouble(v.toString());
                if (value < postProb) {
                    return false;
                }
            }
            if ((percentCoverage != null) && k.getColumnQualifier().toString().equals("percent_coverage")) {
                final Double value = Double.parseDouble(v.toString());
                if (value < percentCoverage) {
                    return false;
                }
            }
        }
        return true;
    }

}