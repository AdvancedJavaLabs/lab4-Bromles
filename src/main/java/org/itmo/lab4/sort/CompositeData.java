package org.itmo.lab4.sort;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CompositeData implements Writable {
    private String category;
    private int quantity;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(category);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category = in.readUTF();
        quantity = in.readInt();
    }
}
