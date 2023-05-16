package database;

import java.util.ArrayList;
import java.util.Iterator;

class Columnimpl implements Column{
    private ArrayList list;
    private String colName;
    int length;
    int nullCount;
    boolean checkString;
    Columnimpl(String name)
    {
        list = new ArrayList<String>();
        colName = name;
        length = name.length();
        nullCount = 0;
        checkString = true;
    }
    @Override
    public String getHeader() {
        return colName;
    }

    @Override
    public String getValue(int index) {
        if (list.get(index)==null) return "null";
        if(list.get(index) instanceof Integer)  return Integer.toString((int)list.get(index));
        else return (String)list.get(index);
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        return null;
    }

    @Override
    public void setValue(int index, String value) {
        try{
            System.out.println("try 진입");
            if(value == null) {
                System.out.println("null 확인");
                list.add(index, null);
                System.out.println("null 삽입");
            }
            else if(value.length()>length) length = value.length();
            int tempInt = Integer.parseInt(value);
            setValue(index,tempInt);
        }
        catch(NumberFormatException e)
        {
            System.out.println("catch 진입");
            list.add(index,value);
        }
    }

    @Override
    public void setValue(int index, int value) {
        list.add(index,value);
    }

    @Override
    public int count() {
        return list.size();
    }

    @Override
    public void show() {

    }

    @Override
    public boolean isNumericColumn() {
        return false;
    }

    @Override
    public long getNullCount() {
        return list.size()-nullCount;
    }
}
