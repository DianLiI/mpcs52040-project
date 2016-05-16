import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

/**
 * Created by dianli on 5/14/16.
 */
public class Investor extends Holder implements Serializable{
    double balance;
    Hashtable<String, Integer> stockTable;
    public Investor(String username, double balance, Hashtable<String, Integer> stockTable) {
        super(username);
        this.balance = balance;
        this.stockTable = stockTable;
    }
}
