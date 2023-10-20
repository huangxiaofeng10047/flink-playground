package org.sample.flink;


import com.ken.parquet.MarketPrice;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
@Component
public class ParquetRunner implements ApplicationRunner {
    @Override
    public void run(ApplicationArguments args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<MarketPrice> marketPriceList = new ArrayList<>();
        for (int i=0;i<100;i++){
            MarketPrice marketPrice = new MarketPrice();
            marketPrice.setPerformanceId("123456789"+i);
            marketPrice.setPriceAsOfDate(100);
            marketPrice.setOpenPrice(100d);
            marketPrice.setHighPrice(120d);
            marketPrice.setLowPrice(99d);
            marketPrice.setClosePrice(101.1d);
            marketPriceList.add(marketPrice);

        }

        DataStream<MarketPrice> marketPriceDataStream = env.fromCollection(marketPriceList);

//        String localPath = "C:\\temp\\flink\\";
        // 写入到s3
        String s3Path = "s3a://warehouse/";
        Path outputPath = new Path(s3Path);
        final FileSink<MarketPrice> sink = FileSink
                .forBulkFormat(outputPath, AvroParquetWriters.forSpecificRecord(MarketPrice.class))
                .build();

//        File outputParquetFile = new File(localPath);

//        String localURI = outputParquetFile.toURI().toString();
//        Path outputPath = new Path(localURI);

//        final FileSink<MarketPrice> sink = FileSink
//                .forBulkFormat(outputPath, AvroParquetWriters.forSpecificRecord(MarketPrice.class))
//                .build();

        marketPriceDataStream.sinkTo(sink);

        marketPriceDataStream.print();

        env.execute();

    }
}

