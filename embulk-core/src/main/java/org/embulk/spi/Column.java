package org.embulk.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.embulk.api.v0.DataType;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;

public class Column implements org.embulk.api.v0.Column {
    @JsonCreator
    public Column(
            @JsonProperty("index") final int index,
            @JsonProperty("name") final String name,
            @JsonProperty("type") final Type type) {
        this.index = index;
        this.name = name;
        this.type = type;

        if (type instanceof BooleanType) {
            this.dataType = DataType.BOOLEAN;
        } else if (type instanceof LongType) {
            this.dataType = DataType.LONG;
        } else if (type instanceof DoubleType) {
            this.dataType = DataType.DOUBLE;
        } else if (type instanceof StringType) {
            this.dataType = DataType.STRING;
        } else if (type instanceof TimestampType) {
            this.dataType = DataType.TIMESTAMP;
        } else if (type instanceof JsonType) {
            this.dataType = DataType.JSON;
        } else {
            throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

    @JsonProperty("index")
    @Override  // From org.embulk.api.v0.Column
    public int getIndex() {
        return index;
    }

    @JsonProperty("name")
    @Override  // From org.embulk.api.v0.Column
    public String getName() {
        return name;
    }

    @JsonProperty("type")
    public Type getType() {
        return type;
    }

    @Override  // From org.embulk.api.v0.Column
    public DataType getDataType() {
        return this.dataType;
    }

    @Override  // From org.embulk.api.v0.Column
    public void visit(final org.embulk.api.v0.ColumnVisitor visitor) {
        switch (this.dataType) {
            case BOOLEAN:
                visitor.booleanColumn(this);
                break;
            case LONG:
                visitor.longColumn(this);
                break;
            case DOUBLE:
                visitor.doubleColumn(this);
                break;
            case STRING:
                visitor.stringColumn(this);
                break;
            case TIMESTAMP:
                visitor.timestampColumn(this);
                break;
            case JSON:
                visitor.jsonColumn(this);
                break;
            default:
                throw new IllegalStateException("Unknown data type: " + this.dataType);
        }
    }

    public void visit(final org.embulk.spi.ColumnVisitor visitor) {
        if (this.type instanceof BooleanType) {
            visitor.booleanColumn(this);
        } else if (this.type instanceof LongType) {
            visitor.longColumn(this);
        } else if (this.type instanceof DoubleType) {
            visitor.doubleColumn(this);
        } else if (this.type instanceof StringType) {
            visitor.stringColumn(this);
        } else if (this.type instanceof TimestampType) {
            visitor.timestampColumn(this);
        } else if (this.type instanceof JsonType) {
            visitor.jsonColumn(this);
        } else {
            assert (false);
        }
    }

    @Override
    public boolean equals(final Object otherObject) {
        if (otherObject == null) {
            return false;
        }
        if (this == otherObject) {
            return true;
        }
        if (!(otherObject instanceof org.embulk.api.v0.Column)) {
            return false;
        }
        final org.embulk.api.v0.Column other = (org.embulk.api.v0.Column) otherObject;
        return Objects.equals(this.index, other.getIndex())
                && Objects.equals(this.name, other.getName())
                && Objects.equals(this.dataType, other.getDataType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.index, this.name, this.type);
    }

    @Override
    public String toString() {
        return String.format("Column{index:%d, name:%s, type:%s}", this.index, this.name, this.dataType.getName());
    }

    private final int index;
    private final String name;
    private final Type type;
    private final org.embulk.api.v0.DataType dataType;
}
