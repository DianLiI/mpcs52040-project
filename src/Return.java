import java.io.Serializable;

/**
 * Created by dianli on 5/14/16.
 */
public class Return implements Serializable{
    String message;
    boolean success;

    public Return(String message, boolean success) {
        this.message = message;
        this.success = success;
    }
}
