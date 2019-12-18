package com.example.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@SpringBootApplication
@EnableBatchProcessing
public class BatchApplication {

    @Value("${application.message}")
    private String message;

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    private static Book bookFromResultSet(ResultSet rs, int i) throws SQLException {
        var book = new Book();
        book.setTitle(rs.getString("title"));
        book.setYear(rs.getInt("year"));
        return book;
    }

    @Bean
    Unmarshaller bookUnmarshaller() {
        var marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(Book.class);
        return marshaller;
    }

    @Bean
    ItemReader xmlItemReader() {
        return new StaxEventItemReaderBuilder<Book>()
                .name("xmlReader")
                .addFragmentRootElements("book")
                .resource(new ClassPathResource("books.xml"))
                .unmarshaller(bookUnmarshaller())
                .build();
    }

    @Bean
    ItemWriter jdbcItemWriter(DataSource ds) {
        return new JdbcBatchItemWriterBuilder<Book>()
                .beanMapped()
                .dataSource(ds)
                .sql("INSERT INTO book(title, year) VALUES (:title, :year);")
                .build();
    }

    @Bean
    ItemWriter csvFileItemWriter() {
        return new FlatFileItemWriterBuilder<Book>()
                .name("fileWriter")
                .resource(new FileSystemResource("/tmp/books.csv"))
                .shouldDeleteIfExists(true)
                .delimited()
                .delimiter(";")
                .names("title", "year")
                .build();
    }


    @Bean
    Step step1(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("step1")
                .tasklet((stepContribution, chunkContext) -> {
                    System.out.println(message);
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    Step step(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("XMLToCSV")
                .<Book, Book>chunk(10)
                .reader(xmlItemReader())
                .writer(jdbcItemWriter(null))
                .build();
    }

    @Bean
    Job job(JobBuilderFactory jobBuilderFactory, JdbcTemplate template) {
        return jobBuilderFactory.get("simpleJob")
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {

                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        template.query("SELECT * FROM book;", BatchApplication::bookFromResultSet)
                                .forEach(System.out::println);
                    }
                })
                .flow(step(null))
                .end()
                .build();
    }
}
