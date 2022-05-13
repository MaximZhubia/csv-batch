package max.dataflow.csv.batch.transforms;

import java.util.Arrays;
import java.util.List;
import lombok.NoArgsConstructor;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;

@NoArgsConstructor
public class RetrieveCsvHeaderTransform
    extends PTransform<PCollection<String>, PCollectionView<List<String>>> {

  private static final long serialVersionUID = 1342649315456420764L;

  @Override
  public PCollectionView<List<String>> expand(PCollection<String> input) {
    return input
        .apply(
            MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                .via(line -> Arrays.asList(line.split(","))))
        .apply(View.asSingleton());
  }
}
