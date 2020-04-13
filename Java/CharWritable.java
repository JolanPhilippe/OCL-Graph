public class CharWritable implements Writable {

    private char c;

    public void write(DataOutput out) throws IOException{
        out.writeChar(c);
    }

    public void readFields(DataInput in) throws IOException{
        c = in.readChar();
    }

    public static CharWritable read(DataInput in) throws IOException{
        CharWritable w = new CharWritable();
        w.readFields(in);
        return w;
    }

}