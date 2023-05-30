package database;

import java.util.ArrayList;

class ColumnImpl implements Column{
    ArrayList list;
    String colName;
    int length;
    int nullCount;
    boolean checkString;
    ColumnImpl(String name)
    {
        list = new ArrayList<String>();
        colName = name;
        length = name.length();
        nullCount = 0;
        checkString = false;
    }
    @Override
    public String getHeader() {
        return colName;
    }

    @Override
    public String getValue(int index) {
        if (list.get(index)==null) return null;
        if(list.get(index) instanceof Integer)  return Integer.toString((int)list.get(index));
        else return (String)list.get(index);
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t)
    {
        try
        {
            int temp = Integer.parseInt(getValue(index));
            return t.cast(temp);
        }
        catch(Exception e)
        {
            double temp = Double.parseDouble(getValue(index));
            return t.cast(temp);
        }
    }

    @Override
    public void setValue(int index, String value) {
        try{
            if(value == null) {
                nullCount++;
                if(index<list.size())
                    list.set(index,null);
                else    list.add(index, null);
                return;
            }
            else if(value.length()>length) length = value.length();
            int tempInt = Integer.parseInt(value); // string 이 문자열이면 int로 변환하지 못해 NFE 발생 (String인 경우 Catch 문으로 이동)
            setValue(index,tempInt); // int로 변환 가능하면 list.add 수행
        }
        catch(NumberFormatException e)
        {
            checkString = true;
            if(index<list.size())
                list.set(index,value);
            else list.add(index,value);
        }
    }

    @Override
    public void setValue(int index, int value) {
        if(index<list.size())
            list.set(index,value);
        else list.add(index,value);
    }

    @Override
    public int count() {
        return list.size();
    }

    @Override
    public void show() {

    }

    @Override
    public boolean isNumericColumn()
    {
        if(checkString)
        {
            for(Object s:list)
            {
                try {Double tempDouble = Double.parseDouble((String)s);}
                catch(Exception e) {return false;}
            }
            return true;
        }
        else return true;
    }

    @Override
    public long getNullCount() {
        return nullCount;
    }
}
