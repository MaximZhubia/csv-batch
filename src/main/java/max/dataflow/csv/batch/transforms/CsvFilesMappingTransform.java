package max.dataflow.csv.batch.transforms;

import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import max.dataflow.csv.batch.functions.CreateKVPCollectionFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

@RequiredArgsConstructor
public class CsvFilesMappingTransform extends PTransform<PBegin, PCollection<String>> {

  private static final long serialVersionUID = 4209158137710992824L;
  private final PCollection<String> csvType1Content;
  private final PCollection<String> csvType2Content;
  private final PCollectionView<Map<Integer, Integer>> columnsMappingView;

  @Override
  public PCollection<String> expand(PBegin input) {

    SerializableFunction<Map<Integer, Integer>, Optional<Integer>> getMapKeyFn =
        x -> x.keySet().stream().findFirst();
    SerializableFunction<Map<Integer, Integer>, Optional<Integer>> getMapValueFn =
        x -> x.values().stream().findFirst();

    PCollection<KV<String, String>> csvType1KVCollection =
        csvType1Content.apply(
            "Group CSV Type1 context by key column",
            ParDo.of(new CreateKVPCollectionFn(columnsMappingView, getMapKeyFn))
                .withSideInputs(columnsMappingView));

    PCollection<KV<String, String>> csvType2KVCollection =
        csvType2Content.apply(
            "Group CSV Type1 context by key column",
            ParDo.of(new CreateKVPCollectionFn(columnsMappingView, getMapValueFn))
                .withSideInputs(columnsMappingView));

    return null;
  }
}
