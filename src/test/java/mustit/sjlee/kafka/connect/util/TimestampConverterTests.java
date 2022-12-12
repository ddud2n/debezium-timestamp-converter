package oryanmoshe.kafka.connect.util;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.debezium.spi.converter.RelationalColumn;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

import java.time.LocalDateTime;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TimestampConverterTests {
  private class MockRegistration<S> implements ConverterRegistration<S> {
    public Converter _converter;
    public S _schema;

    @Override
    public void register(S fieldSchema, Converter converter) {
      _converter = converter;
      _schema = fieldSchema;
    }
  }

  @ParameterizedTest
  @CsvSource({"" +
//        "date, YYYY-MM-dd, 0, 0000-00-00", "date,, 18368, 2020-04-16", "time, mm:ss.SSS, 2230, 00:02.230",
//            "time,, 2230, 00:00:02.230", "datetime, YYYY-MM-dd, 1587042000279, 2020-04-16",
//            "datetime,, 1587042000279, 2020-04-16T13:00:00.279Z", "timestamp, YYYY-MM-dd, 1587042000279, 2020-04-16",
//            "datetime2,, 1587042000279, 2020-04-16T13:00:00.279Z", "datetime2, YYYY-MM-dd, 1587042000279, 2020-04-16",
//            "timestamp,, 1587042000279, 2020-04-16T13:00:00.279Z", "date, YYYY-MM-dd, 2019-04-19, 2019-04-19",
//            "datetime,, 2019-04-19 15:13:20.345123, 2019-04-19T15:13:20.345Z", "time,, 15:13:20, 15:13:20.000",
//            "time,HH:mm:ss, 15:13:20, 15:13:20", "timestamp,, 2019-04-19 15:13:20, 2019-04-19T15:13:20.000Z",
//            "datetime,, 19-Apr-2019 15:13:20.345123, 2019-04-19T15:13:20.345Z",
//            "datetime,, 19/04/2019 15:13:20.345123, 2019-04-19T15:13:20.345Z",
//            "datetime,, 2019-4-19 15:13:20.345123, 2019-04-19T15:13:20.345Z",
//            "datetime2,, 2019-4-19 15:13:20.345123, 2019-04-19T15:13:20.345Z",
      "datetime, 2022-08-10T13:52:11.480Z, 1660107131480",
      "date, 2022-08-10, 19214",
      "time, 12:00:10.123, 43210123",
      "timestamp, 1660107131480, 1660107131480",
      "time, 00:00:00.000, 0",
     // "datetime, 0000-00-00T00:00:00.000Z, null",
     //  "date, 0000-00-00, 0"
  })
  void converterTest(final String columnType, final String input, final String expectedResult) {
    final TimestampConverter tsConverter = new TimestampConverter();

    RelationalColumn mockColumn = getMockColumn(columnType);
    MockRegistration<SchemaBuilder> mockRegistration = new MockRegistration<SchemaBuilder>();
    tsConverter.converterFor(mockColumn, mockRegistration);

    Object actualResult = mockRegistration._converter.convert(input);


    assertEquals(Long.parseLong(expectedResult), actualResult,
        String.format(
            "columnType: %s, input: %s, expected: %s, actual: %s",
            columnType, input, expectedResult, actualResult));
  }

  RelationalColumn getMockColumn(String type) {
    switch (type) {
      case "date":
        return getDateColumn();
      case "time":
        return getTimeColumn();
      case "datetime":
        return getDateTimeColumn();
      case "datetime2":
        return getDateTime2Column();
      case "timestamp":
        return getTimestampColumn();
      default:
        return null;
    }
  }

  RelationalColumn getDateColumn() {
    return new RelationalColumn() {

      @Override
      public String typeName() {
        return "date";
      }

      @Override
      public String name() {
        return "datecolumn";
      }

      @Override
      public String dataCollection() {
        return null;
      }

      @Override
      public String typeExpression() {
        return null;
      }

      @Override
      public OptionalInt scale() {
        return null;
      }

      @Override
      public int nativeType() {
        return 0;
      }

      @Override
      public OptionalInt length() {
        return null;
      }

      @Override
      public int jdbcType() {
        return 0;
      }

      @Override
      public boolean isOptional() {
        return false;
      }

      @Override
      public boolean hasDefaultValue() {
        return false;
      }

      @Override
      public Object defaultValue() {
        return null;
      }
    };
  }

  RelationalColumn getTimeColumn() {
    return new RelationalColumn() {

      @Override
      public String typeName() {
        return "time";
      }

      @Override
      public String name() {
        return "timecolumn";
      }

      @Override
      public String dataCollection() {
        return null;
      }

      @Override
      public String typeExpression() {
        return null;
      }

      @Override
      public OptionalInt scale() {
        return null;
      }

      @Override
      public int nativeType() {
        return 0;
      }

      @Override
      public OptionalInt length() {
        return null;
      }

      @Override
      public int jdbcType() {
        return 0;
      }

      @Override
      public boolean isOptional() {
        return false;
      }

      @Override
      public boolean hasDefaultValue() {
        return false;
      }

      @Override
      public Object defaultValue() {
        return null;
      }
    };
  }

  RelationalColumn getDateTimeColumn() {
    return new RelationalColumn() {

      @Override
      public String typeName() {
        return "datetime";
      }

      @Override
      public String name() {
        return "datetimecolumn";
      }

      @Override
      public String dataCollection() {
        return null;
      }

      @Override
      public String typeExpression() {
        return null;
      }

      @Override
      public OptionalInt scale() {
        return null;
      }

      @Override
      public int nativeType() {
        return 0;
      }

      @Override
      public OptionalInt length() {
        return null;
      }

      @Override
      public int jdbcType() {
        return 0;
      }

      @Override
      public boolean isOptional() {
        return false;
      }

      @Override
      public boolean hasDefaultValue() {
        return true;
      }

      @Override
      public Object defaultValue() {
        return null;
      }
    };
  }

  RelationalColumn getDateTime2Column() {
    return new RelationalColumn() {

      @Override
      public String typeName() {
        return "datetime2";
      }

      @Override
      public String name() {
        return "datetime2column";
      }

      @Override
      public String dataCollection() {
        return null;
      }

      @Override
      public String typeExpression() {
        return null;
      }

      @Override
      public OptionalInt scale() {
        return null;
      }

      @Override
      public int nativeType() {
        return 0;
      }

      @Override
      public OptionalInt length() {
        return null;
      }

      @Override
      public int jdbcType() {
        return 0;
      }

      @Override
      public boolean isOptional() {
        return false;
      }

      @Override
      public boolean hasDefaultValue() {
        return false;
      }

      @Override
      public Object defaultValue() {
        return null;
      }
    };
  }

  RelationalColumn getTimestampColumn() {
    return new RelationalColumn() {

      @Override
      public String typeName() {
        return "timestamp";
      }

      @Override
      public String name() {
        return "timestampcolumn";
      }

      @Override
      public String dataCollection() {
        return null;
      }

      @Override
      public String typeExpression() {
        return null;
      }

      @Override
      public OptionalInt scale() {
        return null;
      }

      @Override
      public int nativeType() {
        return 0;
      }

      @Override
      public OptionalInt length() {
        return null;
      }

      @Override
      public int jdbcType() {
        return 0;
      }

      @Override
      public boolean isOptional() {
        return false;
      }

      @Override
      public boolean hasDefaultValue() {
        return false;
      }

      @Override
      public Object defaultValue() {
        return null;
      }
    };
  }
}