package max.dataflow.csv.batch.transforms;

import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import max.dataflow.csv.batch.CsvBatchOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

@NoArgsConstructor
public class StartCsvBatchJobTransform extends PTransform<PBegin, PDone> {

  private static final long serialVersionUID = 335775202679851951L;

  @Override
  public PDone expand(PBegin input) {

    CsvBatchOptions options = input.getPipeline().getOptions().as(CsvBatchOptions.class);

    // CSV Type1 content
    PCollection<String> csvType1Content =
        input
            .getPipeline()
            .apply(
                "Read SCV Type1 file",
                TextIO.read().from(options.getInputType1Folder() + "/*.scv"));
    // CSV Type1 header view
    PCollectionView<List<String>> csvType1HeaderView =
        csvType1Content
            .apply(Sample.any(1))
            .apply("Get SCV header", new RetrieveCsvHeaderTransform());

    // CSV Type2 content
    PCollection<String> csvType2Content =
        input
            .getPipeline()
            .apply(
                "Read SCV Type1 file",
                TextIO.read().from(options.getInputType1Folder() + "/*.scv"));
    // CSV Type2 header view
    PCollectionView<List<String>> csvType2HeaderView =
        csvType1Content
            .apply(Sample.any(1))
            .apply("Get SCV header", new RetrieveCsvHeaderTransform());

    // Create View<Map<Boolean, KV<Integer, Integer>>> that contains mapping between Type1 and Type2
    // columns:
    // - Boolean indicates that matching is existing;
    // - first Integer is number of column in csvType1Content;
    // - second Integer is number of column in scvType2Content;
    PCollectionView<Map<Integer, Integer>> columnsMappingView =
        input.apply(
            "Map Type1 column to Type2 Transform",
            new CsvColumnsMappingTransform(csvType1HeaderView, csvType2HeaderView));

    PCollection<String> csvFilesMappingPCollection =
        input.apply(
            "CSV files mapping",
            new CsvFilesMappingTransform(csvType1Content, csvType2Content, columnsMappingView));

    return PDone.in(input.getPipeline());
  }
}
