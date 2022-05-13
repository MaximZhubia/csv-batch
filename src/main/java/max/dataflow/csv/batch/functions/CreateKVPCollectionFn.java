package max.dataflow.csv.batch.functions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.StringUtils;

@RequiredArgsConstructor
public class CreateKVPCollectionFn extends DoFn<String, KV<String, String>> {

  private static final long serialVersionUID = -7901953961586532473L;
  private final PCollectionView<Map<Integer, Integer>> columnsMappingView;
  private final SerializableFunction<Map<Integer, Integer>, Optional<Integer>> getColumnNumberFn;

  @ProcessElement
  public void processElement(@Element String row, ProcessContext context) {
    getColumnNumberFn
        .apply(context.sideInput(columnsMappingView))
        .ifPresent(
            column -> {
              List<String> rowValues =
                  Arrays.asList(row.split(",")).stream()
                      .map(String::trim)
                      .collect(Collectors.toList());
              if ((column < rowValues.size()) && StringUtils.isNotEmpty(rowValues.get(column)))
                context.output(KV.of(rowValues.get(column), row));
            });
  }
}
