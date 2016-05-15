import org.jgroups.Address;

import java.io.Serializable;

/**
 * Created by dianli on 5/14/16.
 */
public class AbsAddress implements Serializable{
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setAddress(Address address) {
        this.address = address;
    }

    public String getClusterName() {

        return clusterName;
    }

    public Address getAddress() {
        return address;
    }

    private String clusterName;
    private Address address;

    public AbsAddress(String clusterName, Address address) {
        this.clusterName = clusterName;
        this.address = address;
    }
}
