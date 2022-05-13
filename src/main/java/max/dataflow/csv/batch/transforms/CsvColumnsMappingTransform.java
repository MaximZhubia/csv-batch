package max.dataflow.csv.batch.transforms;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@RequiredArgsConstructor
public class CsvColumnsMappingTransform
    extends PTransform<PBegin, PCollectionView<Map<Integer, Integer>>> {

  private static final long serialVersionUID = -2712772257190154851L;
  private final PCollectionView<List<String>> csvType1HeaderView;
  private final PCollectionView<List<String>> csvType2HeaderView;

  @Override
  public PCollectionView<Map<Integer, Integer>> expand(PBegin input) {
    return input
        .getPipeline()
        .apply(Create.of(StringUtils.EMPTY))
        .apply(
            "Map columns",
            ParDo.of(
                    new DoFn<String, Map<Integer, Integer>>() {
                      private static final long serialVersionUID = 682381227576530221L;

                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        List<String> csvType1Header = context.sideInput(csvType1HeaderView);
                        List<String> csvType2Header = context.sideInput(csvType2HeaderView);
                        for (String header : csvType1Header) {
                          if (csvType2Header.contains(header)) {
                            context.output(
                                Map.of(
                                    csvType1Header.indexOf(header),
                                    csvType2Header.indexOf(header)));
                            log.info(
                                "Columns mapping:: name: {}, columns: {}x{}",
                                header,
                                csvType1Header.indexOf(header),
                                csvType2Header.indexOf(header));
                            return;
                          }
                        }
                        context.output(Map.of());
                        log.info("Columns can not be mapped");
                      }
                    })
                .withSideInputs(csvType1HeaderView, csvType2HeaderView))
        .apply(View.asSingleton());
  }
}
