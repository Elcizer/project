package database;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Predicate;

class Tableimpl implements Table{
    private String tableName;
    private Columnimpl[] columns;

    public Tableimpl(String name,Columnimpl[] col)
    {
        tableName = name.substring(0,name.length()-4);
        columns = col;
    }
    public Tableimpl(Columnimpl[] col)
    {
        columns = col;
    }
    @Override
    public Table crossJoin(Table rightTable) {
        return null;
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        return null;
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void show() {
        for(Columnimpl c:columns) {
            System.out.print(String.format("%"+c.length+"s | ",c.getHeader()));
        }
        System.out.println();
        for(int i=0;i<columns[0].count();i++) {
            for (Columnimpl c : columns) {
                    System.out.print(String.format("%" + c.length + "s | ", c.getValue(i)));
            }
            System.out.println();
        }
    }

    @Override
    public void describe() {
        Columnimpl[] tempCol = new Columnimpl[4];
        tempCol[0] = new Columnimpl("#");
        tempCol[1] = new Columnimpl("Column");
        tempCol[2] = new Columnimpl("Non-Null Count");
        tempCol[3] = new Columnimpl("Dtype");
        for(int i=0;i<columns.length;i++)
            tempCol[0].setValue(i,i);
        for(int i=0;i<columns.length;i++)
            tempCol[1].setValue(i,columns[i].getHeader());
        for(int i=0;i<columns.length;i++)
            tempCol[2].setValue(i,columns[0].count()-columns[i].getNullCount()+" non-null");
        for(int i=0;i<columns.length;i++) {
            if(columns[i].checkString) tempCol[3].setValue(i,"String");
                else tempCol[3].setValue(i,"Int");
            }
        Table tempT = new Tableimpl(tempCol);
        tempT.show();
        }


    @Override
    public Table head() {
        return null;
    }

    @Override
    public Table head(int lineCount) {
        return null;
    }

    @Override
    public Table tail() {
        return null;
    }

    @Override
    public Table tail(int lineCount) {
        return null;
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectRowsAt(int... indices) {
        return null;
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        return null;
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        return null;
    }

    @Override
    public <T> Table selectRowsBy(String columnName, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        return null;
    }

    @Override
    public int getRowCount() {
        return 0;
    }

    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public Column getColumn(int index) {
        return null;
    }

    @Override
    public Column getColumn(String name) {
        return null;
    }
}
