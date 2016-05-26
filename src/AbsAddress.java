import org.jgroups.Address;

import java.io.Serializable;

/**
 * Created by dianli on 5/14/16.
 */
public class AbsAddress implements Serializable{

    public String clusterName;
    public Address address;

    public AbsAddress(String clusterName, Address address) {
        this.clusterName = clusterName;
        this.address = address;
    }
}
