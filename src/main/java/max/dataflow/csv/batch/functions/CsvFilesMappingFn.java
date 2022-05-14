package max.dataflow.csv.batch.functions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

@RequiredArgsConstructor
public class CsvFilesMappingFn extends DoFn<KV<String, CoGbkResult>, String> {

  private static final long serialVersionUID = -7043020885612376573L;
  private final TupleTag<String> csvType1Tuple;
  private final TupleTag<String> csvType2Tuple;

  @ProcessElement
  public void processElement(@Element KV<String, CoGbkResult> element, ProcessContext context) {
    CoGbkResult result = element.getValue();
    List<String> associatedCSVRowsType1 =
        StreamSupport.stream(result.getAll(csvType1Tuple).spliterator(), false)
            .collect(Collectors.toList());
    List<String> associatedCSVRowsType2 =
        StreamSupport.stream(result.getAll(csvType2Tuple).spliterator(), false)
            .collect(Collectors.toList());
    // Make Left Join between CSVRowsType1 and CSVRowsType2
    associatedCSVRowsType1.forEach(
        rowType1 ->
            associatedCSVRowsType2.forEach(
                rowType2 -> context.output(String.join(",", rowType1, rowType2))));
  }
}
