import java.io.Serializable;

/**
 * Created by dianli on 5/14/16.
 */
public class Return implements Serializable{
    String message;
    boolean success;
    Object object; // Any object that need to be send back

    public Return(String message, boolean success) {
        this.message = message;
        this.success = success;
        this.object = null;
    }

    public Return(String message, boolean success, Object object) {
        this.message = message;
        this.success = success;
        this.object = object;
    }
}
