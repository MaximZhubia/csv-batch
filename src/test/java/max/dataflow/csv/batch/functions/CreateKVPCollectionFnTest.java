package max.dataflow.csv.batch.functions;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.jupiter.api.Test;

public class CreateKVPCollectionFnTest implements Serializable {

  private static final long serialVersionUID = -5591624387092512928L;
  private static String CSV_DATA =
      "column_field_0,column_field_1,column_field_2,column_field_3,column_field_4,column_field_5";
  private SerializableFunction<Map<Integer, Integer>, Optional<Integer>> getMapKeyFn =
      x -> x.keySet().stream().findFirst();
  private SerializableFunction<Map<Integer, Integer>, Optional<Integer>> getMapValueFn =
      x -> x.values().stream().findFirst();

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  void testKVWithGetMapKeyFnIsNotEmpty() {
    PCollectionView<Map<Integer, Integer>> columnsMappingView =
        pipeline.apply("Create ColumnsMappingView", Create.of(KV.of(2, 4))).apply(View.asMap());

    PCollection<KV<String, String>> csvKVCollection =
        pipeline
            .apply("Create CSV Data", Create.of(CSV_DATA))
            .apply(
                "Group CSV Type1 context by key column",
                ParDo.of(new CreateKVPCollectionFn(columnsMappingView, getMapKeyFn))
                    .withSideInputs(columnsMappingView));

    PAssert.that(csvKVCollection).containsInAnyOrder(KV.of("column_field_2", CSV_DATA));

    pipeline.run().waitUntilFinish();
  }

  @Test
  void testKVWithGetMapKeyFnIsEmpty() {
    PCollectionView<Map<Integer, Integer>> columnsMappingView =
        pipeline
            .apply(
                "Create ColumnsMappingView",
                Create.empty(KvCoder.of(VarIntCoder.of(), VarIntCoder.of())))
            .apply(View.asMap());

    PCollection<KV<String, String>> csvKVCollection =
        pipeline
            .apply("Create CSV Data", Create.of(CSV_DATA))
            .apply(
                "Group CSV Type1 context by key column",
                ParDo.of(new CreateKVPCollectionFn(columnsMappingView, getMapKeyFn))
                    .withSideInputs(columnsMappingView));

    PAssert.that(csvKVCollection).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  void testKVWithGetMapKeyFnIsOutOfScope() {
    PCollectionView<Map<Integer, Integer>> columnsMappingView =
        pipeline.apply("Create ColumnsMappingView", Create.of(KV.of(12, 4))).apply(View.asMap());

    PCollection<KV<String, String>> csvKVCollection =
        pipeline
            .apply("Create CSV Data", Create.of(CSV_DATA))
            .apply(
                "Group CSV Type1 context by key column",
                ParDo.of(new CreateKVPCollectionFn(columnsMappingView, getMapKeyFn))
                    .withSideInputs(columnsMappingView));

    PAssert.that(csvKVCollection).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  void testKVWithGetMapValueFnIsNotEmpty() {
    PCollectionView<Map<Integer, Integer>> columnsMappingView =
        pipeline.apply("Create ColumnsMappingView", Create.of(KV.of(2, 4))).apply(View.asMap());

    PCollection<KV<String, String>> csvKVCollection =
        pipeline
            .apply("Create CSV Data", Create.of(CSV_DATA))
            .apply(
                "Group CSV Type1 context by key column",
                ParDo.of(new CreateKVPCollectionFn(columnsMappingView, getMapValueFn))
                    .withSideInputs(columnsMappingView));

    PAssert.that(csvKVCollection).containsInAnyOrder(KV.of("column_field_4", CSV_DATA));

    pipeline.run().waitUntilFinish();
  }
}
