package dto;

public class Curator {
  private int id = -1;
  private String fio = "";

  public Curator (int id, String fio) {
    this.id = id;
    this.fio = fio;
  }

  public int getId() {
    return id;
  }

  public String getFIO() {
    return fio;
  }

}