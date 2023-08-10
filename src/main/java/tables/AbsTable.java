package tables;

import data.AggregationData;
import data.PredicatesData;
import db.DBConnector;
import db.IDBConnector;
import dto.Student;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public abstract class AbsTable {
  public String tableName;
  private IDBConnector idbConnector;

  public AbsTable(String tableName, IDBConnector idbConnector) {
    this.tableName = tableName;
    this.idbConnector = idbConnector;
  }

  public void create(List<String> columns) {
    idbConnector.executeRequest(String.format("create table %s (%s);", tableName, String.join(",", columns)));
  }


  public void delete() {
    idbConnector.executeRequest(String.format("drop table %s", tableName));
  }

  public void insertData() {
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('1', 'Васильев Юстин Игнатьевич', 'M', '1'); ");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('2', 'Мясников Вячеслав Васильевич', 'M', '2');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('3', 'Михеева Динара Васильевна', 'F', '3');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('4', 'Беляева Нева Станиславовна', 'F', '1');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('5', 'Терентьев Андрей Мэлорович', 'M', '2');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('6', 'Фомичёва Амелия Мартыновна', 'F', '3');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('7', 'Корнилов Всеволод Платонович', 'M', '1');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('8', 'Игнатьева Валерия Эдуардовна', 'F', '2');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('9', 'Соколова Дина Дмитриевна', 'F', '3');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('10', 'Комаров Исаак Филатович', 'M', '1');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('11', 'Семёнова Тамара Валерьяновна', 'F', '2');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('12', 'Котов Велор Дамирович', 'M', '3');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('13', 'Константинова Сандра Сергеевна', 'F', '1');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('14', 'Лобанова Эвелина Львовна', 'F', '2');");
    idbConnector.executeRequest("INSERT INTO `otus`.`student`(`id`, `fio`, `sex`, `id_group`) VALUES('15', 'Логинов Архип Проклович', 'M', '3');");
    idbConnector.executeRequest("INSERT INTO `otus`.`group_s` (`id`, `name`, `id_curator`) VALUES ('1', 'Первая', '1');");
    idbConnector.executeRequest("INSERT INTO `otus`.`group_s` (`id`, `name`, `id_curator`) VALUES ('2', 'Вторая', '2');");
    idbConnector.executeRequest("INSERT INTO `otus`.`group_s` (`id`, `name`, `id_curator`) VALUES ('3', 'Третья', '3');");
    idbConnector.executeRequest("INSERT INTO `otus`.`curator` (`id`, `fio`) VALUES ('1', 'Карпов Гордей Филатович');");
    idbConnector.executeRequest("INSERT INTO `otus`.`curator` (`id`, `fio`) VALUES ('2', 'Лапина Амина Максимовна');");
    idbConnector.executeRequest("INSERT INTO `otus`.`curator` (`id`, `fio`) VALUES ('3', 'Васильев Вениамин Сергеевич');");
    idbConnector.executeRequest("INSERT INTO `otus`.`curator` (`id`, `fio`) VALUES ('4', 'Мамонтова Хильда Михайловна');");

  }

  public ResultSet getData(String[] columns, Map<PredicatesData, List<String>> predicates, AggregationData aggregate, Map<String, List<String>> joins) {
    String columnRequest = columns.length == 0 ? "*" : String.join(", ", columns);
    if (aggregate == AggregationData.COUNT) columnRequest = "count(" + columnRequest + ")";
    String joinResult = "";
    if (joins != null) {
      if (joins.size() == 1) {
        if (joins.entrySet().size() == 1) {
          List<Map.Entry<String, List<String>>> joinTablesPredicate = new ArrayList();
          joinTablesPredicate.addAll(joins.entrySet());
          joinResult = joinTablesPredicate.get(0).getKey() + " on " + joinTablesPredicate.get(0).getValue().get(0);
        }
      }
      else {
        for (Map.Entry<String, List<String>> join : joins.entrySet()) {
          joinResult += join.getKey()  + " on " +  join.getValue().get(0) + " ";
        }
      }
    }
    String result = "";
    if (predicates != null) {
      if (predicates.size() == 1) {
        if (predicates.entrySet().size() == 1) {
          List<Map.Entry<PredicatesData, List<String>>> predicateList = new ArrayList();
          predicateList.addAll(predicates.entrySet());
          result = predicateList.get(0).getValue().get(0);
        }
      }

      for (Map.Entry<PredicatesData, List<String>> predicate : predicates.entrySet()) {
        if (predicate.getValue().size() > 1) {
          result += String.join(String.format(" %s ", predicate.getKey().name()), predicate.getValue());
        }
      }
    }
    if (!result.isEmpty()) {
      result = "where " + result;
    }

    if (joins != null) {
      String sqlRequest = String.format("select %s from %s %s %s;", columnRequest, tableName, joinResult, result);
      System.out.println(sqlRequest);

      return idbConnector.execute(sqlRequest);
    } else {
      String sqlRequest = String.format("select %s from %s %s;", columnRequest, tableName, result);

      System.out.println(sqlRequest);

      return idbConnector.execute(sqlRequest);
    }
  }

  public void updateData(List<String> updateValues, Map<PredicatesData, List<String>> predicates) {
    String updatedValues = String.join(", ", updateValues);

    String conditions = "";
    if (predicates != null) {
      if (predicates.size() == 1) {
        if (predicates.entrySet().size() == 1) {
          List<Map.Entry<PredicatesData, List<String>>> predicateList = new ArrayList();
          predicateList.addAll(predicates.entrySet());
          conditions = predicateList.get(0).getValue().get(0);

        }
      }

      for (Map.Entry<PredicatesData, List<String>> predicate : predicates.entrySet()) {
        if (predicate.getValue().size() > 1) {
          conditions += String.join(String.format(" %s ", predicate.getKey().name()), predicate.getValue());
        }

      }
      if (!conditions.isEmpty()) {
        conditions = "where " + conditions;
      }
    }


    String updateRequest = String.format("update %s set %s %s;", tableName, updatedValues, conditions);

    System.out.println(updateRequest);

    idbConnector.executeRequest(updateRequest);
  }

  public void close() {
    idbConnector.close();
  }
}