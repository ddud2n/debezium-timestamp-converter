package oryanmoshe.kafka.connect.util;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
// java.time.LocalDate ->  Date 타입
// java.sql.Timestamp -> Timestamp 타입
// java.lang.Long
// java.sql.Timestamp -> Datetime 타입

import org.apache.kafka.connect.data.SchemaBuilder;

public class TimestampConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

  public static final List<String> SUPPORTED_DATA_TYPES = List.of("date", "time", "datetime", "timestamp", "datetime2");
  public static final long MILLISECONDS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);
  public static final long MICROSECONDS_PER_SECOND = TimeUnit.SECONDS.toMicros(1);
  public static final long MICROSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toMicros(1);
  public static final long NANOSECONDS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
  public static final long NANOSECONDS_PER_MICROSECOND = TimeUnit.MICROSECONDS.toNanos(1);
  public static final long NANOSECONDS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  public static final long NANOSECONDS_PER_DAY = TimeUnit.DAYS.toNanos(1);
  public static final long SECONDS_PER_DAY = TimeUnit.DAYS.toSeconds(1);
  public static final long MICROSECONDS_PER_DAY = TimeUnit.DAYS.toMicros(1);
  public static final LocalDate EPOCH = LocalDate.ofEpochDay(0);
  public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
  public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss.SSS";

  @Override
  public void configure(Properties props) {

  }

  @Override
  public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
    String sqlType = column.typeName().toLowerCase();
    if (SUPPORTED_DATA_TYPES.stream().anyMatch(s -> s.equalsIgnoreCase(sqlType))) {

      registration.register(SchemaBuilder.int64().optional(), rawValue -> {
        try {
          if (rawValue == null) return null;
          if (rawValue instanceof Long) {if((long)rawValue == 0) return null; else return (long) rawValue;}
          if (rawValue instanceof String) return (long) StringToTime((String) rawValue, sqlType);
          switch (sqlType) {
            case "date":
              LocalDate localDate = toLocalDate(rawValue);
              return (long) localDate.toEpochDay();
            case "time":
              LocalTime time = toLocalTime(rawValue);
              return (long) Math.floorDiv(time.toNanoOfDay(), NANOSECONDS_PER_MILLISECOND);
            case "timestamp":
              if (rawValue instanceof java.sql.Timestamp) {
                java.sql.Timestamp timestamp = (java.sql.Timestamp) rawValue;
                timestamp.setTime(timestamp.getTime() + 9 * 60 * 60 * 1000); // 한국시간 : +9
                LocalDateTime dateTime = toLocalDateTime(timestamp);
                return (long) dateTime.toInstant(ZoneOffset.of("+09:00")).toEpochMilli();
              }
            default:
              LocalDateTime dateTime = toLocalDateTime(rawValue);
              return (long) dateTime.toInstant(ZoneOffset.of("+09:00")).toEpochMilli();
          }
        } catch (Exception e) {
          return null;
        }
      });

    }
  }

  public static LocalDateTime toLocalDateTime(Object obj) {

    if (obj == null) {
      return null;
    }
    if (obj instanceof OffsetDateTime) {
      return ((OffsetDateTime) obj).toLocalDateTime();
    }
    if (obj instanceof Instant) {
      return ((Instant) obj).atOffset(ZoneOffset.of("+09:00")).toLocalDateTime();
    }
    if (obj instanceof LocalDateTime) {
      return (LocalDateTime) obj;
    }
    if (obj instanceof LocalDate) {
      LocalDate date = (LocalDate) obj;
      return LocalDateTime.of(date, LocalTime.MIDNIGHT);
    }
    if (obj instanceof LocalTime) {
      LocalTime time = (LocalTime) obj;
      return LocalDateTime.of(EPOCH, time);
    }
    if (obj instanceof java.sql.Date) {
      java.sql.Date sqlDate = (java.sql.Date) obj;
      LocalDate date = sqlDate.toLocalDate();
      return LocalDateTime.of(date, LocalTime.MIDNIGHT);
    }
    if (obj instanceof java.sql.Time) {
      LocalTime localTime = toLocalTime(obj);
      return LocalDateTime.of(EPOCH, localTime);
    }
    if (obj instanceof java.sql.Timestamp) {
      java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
      return LocalDateTime.of(timestamp.getYear() + 1900,
          timestamp.getMonth() + 1,
          timestamp.getDate(),
          timestamp.getHours(),
          timestamp.getMinutes(),
          timestamp.getSeconds(),
          timestamp.getNanos());
    }
    if (obj instanceof java.util.Date) {
      java.util.Date date = (java.util.Date) obj;
      long millis = (int) (date.getTime() % MILLISECONDS_PER_SECOND);
      if (millis < 0) {
        millis = MILLISECONDS_PER_SECOND + millis;
      }
      int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
      return LocalDateTime.of(date.getYear() + 1900,
          date.getMonth() + 1,
          date.getDate(),
          date.getHours(),
          date.getMinutes(),
          date.getSeconds(),
          nanosOfSecond);
    }
    throw new IllegalArgumentException("Unable to convert to LocalTime from unexpected value '" + obj + "' of type " + obj.getClass().getName());
  }

  public static LocalTime toLocalTime(Object obj) {
    if (obj == null) {
      return null;
    }
    if (obj instanceof LocalTime) {
      return (LocalTime) obj;
    }
    if (obj instanceof LocalDateTime) {
      return ((LocalDateTime) obj).toLocalTime();
    }
    if (obj instanceof java.sql.Date) {
      throw new IllegalArgumentException("Unable to convert to LocalDate from a java.sql.Date value '" + obj + "'");
    }
    if (obj instanceof java.sql.Time) {
      java.sql.Time time = (java.sql.Time) obj;
      long millis = (int) (time.getTime() % MILLISECONDS_PER_SECOND);
      int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
      return LocalTime.of(time.getHours(),
          time.getMinutes(),
          time.getSeconds(),
          nanosOfSecond);
    }
    if (obj instanceof java.sql.Timestamp) {
      java.sql.Timestamp timestamp = (java.sql.Timestamp) obj;
      return LocalTime.of(timestamp.getHours(),
          timestamp.getMinutes(),
          timestamp.getSeconds(),
          timestamp.getNanos());
    }
    if (obj instanceof java.util.Date) {
      java.util.Date date = (java.util.Date) obj;
      long millis = (int) (date.getTime() % MILLISECONDS_PER_SECOND);
      int nanosOfSecond = (int) (millis * NANOSECONDS_PER_MILLISECOND);
      return LocalTime.of(date.getHours(),
          date.getMinutes(),
          date.getSeconds(),
          nanosOfSecond);
    }
    if (obj instanceof Duration) {
      Long value = ((Duration) obj).toNanos();
      if (value >= 0 && value <= NANOSECONDS_PER_DAY) {
        return LocalTime.ofNanoOfDay(value);
      } else {
        throw new IllegalArgumentException("Time values must use number of milliseconds greater than 0 and less than 86400000000000");
      }
    }
    throw new IllegalArgumentException("Unable to convert to LocalTime from unexpected value '" + obj + "' of type " + obj.getClass().getName());
  }

  public static LocalDate toLocalDate(Object obj) {
    if (obj == null) {
      return null;
    }
    if (obj instanceof LocalDate) {
      return (LocalDate) obj;
    }
    if (obj instanceof LocalDateTime) {
      return ((LocalDateTime) obj).toLocalDate();
    }
    if (obj instanceof java.sql.Date) {
      return ((java.sql.Date) obj).toLocalDate();
    }
    if (obj instanceof java.sql.Time) {
      throw new IllegalArgumentException("Unable to convert to LocalDate from a java.sql.Time value '" + obj + "'");
    }
    if (obj instanceof java.util.Date) {
      java.util.Date date = (java.util.Date) obj;
      return LocalDate.of(date.getYear() + 1900,
          date.getMonth() + 1,
          date.getDate());
    }
    if (obj instanceof Long) {
      // Assume the value is the epoch day number
      return LocalDate.ofEpochDay((Long) obj);
    }
    if (obj instanceof Integer) {
      // Assume the value is the epoch day number
      return LocalDate.ofEpochDay((Integer) obj);
    }
    throw new IllegalArgumentException("Unable to convert to LocalDate from unexpected value '" + obj + "' of type " + obj.getClass().getName());
  }

  public static Long StringToTime(String str, String sqlType) {
    try {
      // 문자열이 숫자인지 확인
      if (str.matches("[+-]?\\d*(\\.\\d+)?")) return Long.parseLong(str);

      switch (sqlType) {
        case "date":
          DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT);
          LocalDate localDate = LocalDate.parse(str, dateFormatter);
          return (long) localDate.toEpochDay();
        case "time":
          DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(DEFAULT_TIME_FORMAT);
          LocalTime time = LocalTime.parse(str, timeFormatter);
          return (long) Math.floorDiv(time.toNanoOfDay(), NANOSECONDS_PER_MILLISECOND);
        default:
          DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT);
          LocalDateTime dateTime = LocalDateTime.parse(str, dateTimeFormatter);
          return (long) dateTime.toInstant(ZoneOffset.of("+09:00")).toEpochMilli();
      }
    } catch (Exception e) {
      return null;
    }
  }

}
