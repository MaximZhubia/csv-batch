package max.dataflow.csv.batch.transforms;

import java.util.List;
import java.util.Map;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import max.dataflow.csv.batch.CsvBatchOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

@Slf4j
@NoArgsConstructor
public class StartCsvBatchJobTransform extends PTransform<PBegin, PDone> {

  private static final long serialVersionUID = 335775202679851951L;

  @Override
  public PDone expand(PBegin input) {

    CsvBatchOptions options = input.getPipeline().getOptions().as(CsvBatchOptions.class);
    String filePath1 = options.getInputType1Folder() + "/*.csv";
    String filePath2 = options.getInputType2Folder() + "/*.csv";

    // CSV Type1 content
    PCollection<String> csvType1Content =
        input.getPipeline().apply("Read CSV Type1 files", TextIO.read().from(filePath1));

    // CSV Type1 header view
    PCollectionView<List<String>> csvType1HeaderView =
        csvType1Content.apply(
            "Get CSV header Type1 files", new RetrieveCsvHeaderTransform(filePath1));

    // CSV Type2 content
    PCollection<String> csvType2Content =
        input.getPipeline().apply("Read CSV Type2 files", TextIO.read().from(filePath2));

    // CSV Type2 header view
    PCollectionView<List<String>> csvType2HeaderView =
        csvType2Content.apply(
            "Get CSV header Type2 files", new RetrieveCsvHeaderTransform(filePath2));

    // Create View<Map<Boolean, KV<Integer, Integer>>> that contains mapping between Type1 and Type2
    // columns:
    // - Boolean indicates that matching is existing;
    // - first Integer is number of column in csvType1Content;
    // - second Integer is number of column in scvType2Content;
    PCollectionView<Map<Integer, Integer>> columnsMappingView =
        input.apply(
            "Map Type1 column to Type2 Transform",
            new CsvColumnsMappingTransform(csvType1HeaderView, csvType2HeaderView));

    // Create associated CSV Rows
    PCollection<String> csvFilesMappingPCollection =
        input.apply(
            "CSV files mapping",
            new CsvFilesMappingTransform(csvType1Content, csvType2Content, columnsMappingView));

    // Write result file
    return csvFilesMappingPCollection.apply(
        "Write CSV result file",
        TextIO.write().to(options.getOutputFolder() + "/associatedResult.csv").withoutSharding());
  }
}
