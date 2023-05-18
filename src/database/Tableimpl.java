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
    public Table head()
    {
        return head(5);
    }

    @Override
    public Table head(int lineCount)
    {
        Columnimpl[] temp = newColumn();
        lineCount = lineCount > columns[0].count() ? columns[0].count() : lineCount;
        for(int i = 0;i<lineCount;i++)
        {
            for(int j = 0 ;j<columns.length;j++)
                temp[j].setValue(i,columns[j].getValue(i));
        }
        return new Tableimpl(temp);
    }

    @Override
    public Table tail() {
        return tail(5);
    }

    @Override
    public Table tail(int lineCount)
    {
        Columnimpl[] temp = newColumn();
        int count = columns[0].count() - lineCount;
        count = count > 0 ? count : 0;
        for(int i = count; i < columns[0].count(); i++) // 행 개수
        {
            for(int j = 0 ;j <columns.length; j++) // 열 개수
                temp[j].setValue(i-count,columns[j].getValue(i));
        }
        return new Tableimpl(temp);
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex)
    {
        Columnimpl[] temp = newColumn();
        if(beginIndex < 0)
        {
            System.out.println("beginIndex 값이 가능한 값을 초과하여 "+ beginIndex+" -> "+0+" 으로 수정되었습니다.");
            beginIndex = 0;
        }
        if(endIndex > columns[0].count())
        {
            System.out.println("endIndex 값이 정해진 값을 초과하여 "+ endIndex+" -> "+columns[0].count()+" 으로 수정되었습니다.");
            endIndex = columns[0].count();
        }
        for(int i=beginIndex;i<endIndex;i++)
        {
            for(int j = 0 ;j <columns.length; j++) // 열 개수
                temp[j].setValue(i,columns[j].getValue(i));
        }
        return new Tableimpl(temp);
    }

    @Override
    public Table selectRowsAt(int... indices)
    {
        int count = 0;
        Columnimpl[] temp = newColumn();
        for(int a : indices)
        {
            if(a<0 || a>columns[0].count()) {System.out.println(indices+": 존재하지 않는 index"); break;}
            for(int i=0;i<columns.length;i++)
                temp[i].setValue(count,columns[i].getValue(a));
            count++;
        }
        return new Tableimpl(temp);
    }

    @Override
    public Table selectColumns(int beginIndex, int endIndex) {
        if(beginIndex < 0)
        {
            System.out.println("beginIndex 값이 가능한 값을 초과하여 "+ beginIndex+" -> "+0+" 으로 수정되었습니다.");
            beginIndex = 0;
        }
        if(endIndex > columns.length)
        {
            System.out.println("endIndex 값이 정해진 값을 초과하여 "+ endIndex+" -> "+columns.length+" 으로 수정되었습니다.");
            endIndex = columns[0].count();
        }
        Columnimpl[] temp = new Columnimpl[endIndex-beginIndex];
        int count = 0;
        for(int i=beginIndex;i<endIndex;i++)
        {
            temp[count++] = new Columnimpl(columns[i].getHeader());
        }
        for(int i=0;i<columns[0].count();i++)
        {
            for(int j=beginIndex;j<endIndex;j++)
            {
                temp[j-beginIndex].setValue(i,columns[j].getValue(i));
            }
        }
        return new Tableimpl(temp);
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        Columnimpl[] temp = new Columnimpl[indices.length];
        int count = 0;
        for(int a : indices)
        {
            if(a<0 || a>columns.length) {System.out.println(indices+": 존재하지 않는 index"); break;}
            temp[count] = new Columnimpl(columns[a].getHeader());
            for(int i=0;i<columns[0].count();i++)
                temp[count].setValue(i,columns[a].getValue(i));
            count++;
        }
        return new Tableimpl(temp);
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
        return columns[index];
    }

    @Override
    public Column getColumn(String name) {
        for(Columnimpl c:columns)
        {
            if(name.equals(c.getHeader())) return c;
        }
        return null;
    }
    private Columnimpl[] newColumn()
    {
        Columnimpl[] temp = new Columnimpl[columns.length];
        for(int i = 0;i<columns.length;i++)
            temp[i] = new Columnimpl(columns[i].getHeader());
        return temp;
    }
}
