package io.pivotal.pde.sample;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionResolver;

public class UnitTelemetryPartitionKey implements PartitionResolver, Declarable {

    private String id;
    private String vin;

    @Override
    public Object getRoutingObject(EntryOperation entryOperation) {

        id = (String)entryOperation.getKey();
        vin = id.substring(0, id.indexOf("|"));
        return vin;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UnitTelemetryPartitionKey)) return false;

        UnitTelemetryPartitionKey that = (UnitTelemetryPartitionKey) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return vin != null ? vin.equals(that.vin) : that.vin == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (vin != null ? vin.hashCode() : 0);
        return result;
    }

    @Override
    public String getName() {
        return "UnitTelemetryPartitionKey";
    }

    @Override
    public void close() {

    }

    @Override
    public void init(Properties properties) {

    }
}