package org.embulk.spi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.embulk.api.v0.DataType;
import org.embulk.spi.type.Type;

public class Schema implements org.embulk.api.v0.Schema {
    public static class Builder {
        public Builder() {
            this.columnsBuilt = new ArrayList<>();
            this.index = 0;
        }

        public synchronized Builder add(final String name, final Type type) {
            columnsBuilt.add(new Column(this.index++, name, type));
            return this;
        }

        public Schema build() {
            return new Schema(Collections.unmodifiableList(this.columnsBuilt));
        }

        private final ArrayList<Column> columnsBuilt;
        private int index;
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonCreator
    public Schema(final List<Column> columns) {
        this.columns = Collections.unmodifiableList(new ArrayList<Column>(columns));
    }

    /**
     * Returns the list of Column objects.
     *
     * It always returns an immutable list.
     */
    @JsonValue
    @Override  // From org.embulk.api.v0.Schema
    public List<Column> getColumns() {
        return this.columns;
    }

    @Override  // From org.embulk.api.v0.Schema
    public int size() {
        return this.columns.size();
    }

    @Override  // From org.embulk.api.v0.Schema
    public int getColumnCount() {
        return this.columns.size();
    }

    @Override  // From org.embulk.api.v0.Schema
    public Column getColumn(final int index) {
        return this.columns.get(index);
    }

    @Override  // From org.embulk.api.v0.Schema
    public String getColumnName(final int index) {
        return this.getColumn(index).getName();
    }

    @Override  // From org.embulk.api.v0.Schema
    public DataType getColumnDataType(final int index) {
        return this.getColumn(index).getDataType();
    }

    public Type getColumnType(final int index) {
        return this.getColumn(index).getType();
    }

    @Override  // From org.embulk.api.v0.Schema
    public void visitColumns(final org.embulk.api.v0.ColumnVisitor visitor) {
        for (final Column column : this.columns) {
            column.visit(visitor);
        }
    }

    public void visitColumns(final org.embulk.spi.ColumnVisitor visitor) {
        for (final Column column : this.columns) {
            column.visit(visitor);
        }
    }

    @Override  // From org.embulk.api.v0.Schema
    public boolean isEmpty() {
        return this.columns.isEmpty();
    }

    @Override  // From org.embulk.api.v0.Schema
    public Column lookupColumn(final String name) {
        for (final Column column : this.columns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        throw new SchemaConfigException(String.format("Column '%s' is not found", name));
    }

    // Not overridden intentionally.
    public int getFixedStorageSize() {
        int total = 0;
        for (final Column column : this.columns) {
            total += column.getType().getFixedStorageSize();
        }
        return total;
    }

    @Override
    public boolean equals(final Object otherObject) {
        if (otherObject == null) {
            return false;
        }
        if (this == otherObject) {
            return true;
        }
        if (!(otherObject instanceof org.embulk.api.v0.Schema)) {
            return false;
        }
        final org.embulk.api.v0.Schema other = (org.embulk.api.v0.Schema) otherObject;
        return Objects.equals(this.columns, other.getColumns());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.columns);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("Schema{\n");
        for (final Column column : columns) {
            builder.append(String.format(" %4d: %s %s%n", column.getIndex(), column.getName(), column.getType()));
        }
        builder.append("}");
        return builder.toString();
    }

    private final List<Column> columns;
}
