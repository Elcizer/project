package database;

import java.util.ArrayList;

class Columnimpl implements Column{
    private ArrayList list;
    Columnimpl()
    {
        list = new ArrayList<>();
    }
    @Override
    public String getHeader() {

        return null;
    }

    @Override
    public String getValue(int index) {
        return null;
    }

    @Override
    public <T extends Number> T getValue(int index, Class<T> t) {
        return null;
    }

    @Override
    public void setValue(int index, String value) {
        list.add(index,value);
    }

    @Override
    public void setValue(int index, int value) {
        list.add(index,value);
    }

    @Override
    public int count() {
        return 0;
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
        return 0;
    }
}
