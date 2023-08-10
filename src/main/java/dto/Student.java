package dto;

public class Student {
  private int id = -1;
  private String fio = "";
  private String sex = "";
  private int id_group = -1;

  public Student (int id, String fio, String sex, int id_group) {
    this.id = id;
    this.fio = fio;
    this.sex = sex;
    this.id_group = id_group;
  }

  public int getId() {
    return id;
  }

  public String getFIO() {
    return fio;
  }

  public String getSex() {return sex;}
  public int getIdGroup() {return id_group;}
}