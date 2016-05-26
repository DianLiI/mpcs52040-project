import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;

public class Transaction implements Serializable {

  Date date;
  int tid;
  String holder;
  String stock;
  int amount;
  String type;
  boolean status;

  public Transaction(Date date, int tid, String holder, String stock, int amount, String type, boolean status){
    this.date = date;
    this.tid = tid;
    this.holder = holder;
    this.stock = stock;
    this.amount = amount;
    this.type = type;
    this.status = status;
  }
}
