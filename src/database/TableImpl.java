package database;

import java.util.*;
import java.util.function.Predicate;

class TableImpl implements Table{
    private String tableName;
    private ColumnImpl[] columns;

    @Override
    public String toString() {
        return "<database.Table@"+Integer.toHexString(hashCode())+">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableImpl tableimpl = (TableImpl) o;
        return Objects.equals(tableName, tableimpl.tableName);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableName);
        return result;
    }

    TableImpl(String name, ColumnImpl[] col)
    {
        tableName = name.substring(0,name.length()-4);
        columns = col;
    }
     TableImpl(ColumnImpl[] col)
    {
        columns = col;
    }
    @Override
    public Table crossJoin(Table rightTable) {
        ColumnImpl[] temp = new ColumnImpl[this.getColumnCount()+rightTable.getColumnCount()];
        int count = 0;
        for(int i=0;i<this.getColumnCount();i++)
            temp[count++] = new ColumnImpl(this.getName()+"."+this.getColumn(i).getHeader());
        for(int i=0;i< rightTable.getColumnCount();i++)
            temp[count++] = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader()); // header 초기화
        int rowCount = 0;
        int columnCount;
        for(int i=0;i<this.getRowCount();i++) // leftTable 행 for 문
        {
            for(int j=0;j<rightTable.getRowCount();j++) // rightTable 행 for 문
            {
                columnCount = 0;
                    for(int t1=0;t1<this.getColumnCount();t1++)
                        temp[columnCount++].setValue(rowCount,this.getColumn(t1).getValue(i));
                    for(int t2=0;t2<rightTable.getColumnCount();t2++)
                        temp[columnCount++].setValue(rowCount,rightTable.getColumn(t2).getValue(j));
                    rowCount++;
            }
        }
        return new TableImpl(temp);
    }

    @Override
    public Table innerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        ColumnImpl[] temp = new ColumnImpl[this.getColumnCount()+rightTable.getColumnCount()];
        int count = 0;
        for(int i=0;i<this.getColumnCount();i++)
            temp[count++] = new ColumnImpl(this.getName()+"."+this.getColumn(i).getHeader());
        for(int i=0;i< rightTable.getColumnCount();i++)
            temp[count++] = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader()); // header 초기화
        int rowCount = 0;
        int columnCount;
        for(int i=0;i<this.getRowCount();i++) // leftTable 행 for 문
        {
            for(int j=0;j<rightTable.getRowCount();j++) // rightTable 행 for 문
            {
                boolean check = true;
                for(JoinColumn join : joinColumns)
                {
                    ColumnImpl col1 = (ColumnImpl) this.getColumn(join.getColumnOfThisTable());
                    ColumnImpl col2 = (ColumnImpl) rightTable.getColumn(join.getColumnOfAnotherTable());

                    if(col1.getValue(i)==null || col2.getValue(j)==null) // 비교 값에 null 이 존재할 때
                    {
                        if(col1.getValue(i)!=col2.getValue(j)) {
                            check = false;
                            break;
                        }
                    }
                    else if(!(col1.getValue(i).equals(col2.getValue(j))))
                    {
                        check = false;
                        break;
                    }
                }
                if(check==false) continue;
                columnCount = 0;
                for(int t1=0;t1<this.getColumnCount();t1++)
                    temp[columnCount++].setValue(rowCount,this.getColumn(t1).getValue(i));
                for(int t2=0;t2<rightTable.getColumnCount();t2++)
                    temp[columnCount++].setValue(rowCount,rightTable.getColumn(t2).getValue(j));
                rowCount++;
            }
        }
        return new TableImpl(temp);
    }

    @Override
    public Table outerJoin(Table rightTable, List<JoinColumn> joinColumns) {
        ColumnImpl[] temp = new ColumnImpl[this.getColumnCount()+rightTable.getColumnCount()];
        int count = 0;
        for(int i=0;i<this.getColumnCount();i++)
            temp[count++] = new ColumnImpl(this.getName()+"."+this.getColumn(i).getHeader());
        for(int i=0;i< rightTable.getColumnCount();i++)
            temp[count++] = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader()); // header 초기화
        int rowCount = 0;
        int columnCount;
        boolean[] checkOuterL = new boolean[this.getRowCount()];
        for(int i=0;i<this.getRowCount();i++) // leftTable 행 for 문
        {
            for(int j=0;j<rightTable.getRowCount();j++) // rightTable 행 for 문
            {
                boolean check = true;
                for(JoinColumn join : joinColumns)
                {
                    ColumnImpl col1 = (ColumnImpl) this.getColumn(join.getColumnOfThisTable());
                    ColumnImpl col2 = (ColumnImpl) rightTable.getColumn(join.getColumnOfAnotherTable());

                    if(col1.getValue(i)==null || col2.getValue(j)==null) // 비교 값에 null 이 존재할 때
                    {
                        if(col1.getValue(i)!=col2.getValue(j))
                        {
                            check = false;
                            break;
                        }
                    }
                    else if(!(col1.getValue(i).equals(col2.getValue(j))))
                    {
                        check = false;
                        break;
                    }
                }
                if(check==false) continue;
                checkOuterL[i] = true;
                columnCount = 0;
                for(int t1=0;t1<this.getColumnCount();t1++)
                    temp[columnCount++].setValue(rowCount,this.getColumn(t1).getValue(i));
                for(int t2=0;t2<rightTable.getColumnCount();t2++)
                    temp[columnCount++].setValue(rowCount,rightTable.getColumn(t2).getValue(j));
                rowCount++;
            }
        }
        for(int i=0;i<this.getRowCount();i++)
        {
            if(checkOuterL[i]==false)
            {
                columnCount =0;
                for(int t1=0;t1<this.getColumnCount();t1++)
                    temp[columnCount++].setValue(rowCount,this.getColumn(t1).getValue(i));
                for(int t2=0;t2<rightTable.getColumnCount();t2++)
                    temp[columnCount++].setValue(rowCount,null);
                rowCount++;
            }
        }
        return new TableImpl(temp);
    }

    @Override
    public Table fullOuterJoin(Table rightTable, List<JoinColumn> joinColumns) {
        ColumnImpl[] temp = new ColumnImpl[this.getColumnCount()+rightTable.getColumnCount()];
        int count = 0;
        for(int i=0;i<this.getColumnCount();i++)
            temp[count++] = new ColumnImpl(this.getName()+"."+this.getColumn(i).getHeader());
        for(int i=0;i< rightTable.getColumnCount();i++)
            temp[count++] = new ColumnImpl(rightTable.getName()+"."+rightTable.getColumn(i).getHeader()); // header 초기화
        int rowCount = 0;
        int columnCount;
        boolean[] checkOuterL = new boolean[this.getRowCount()];
        boolean[] checkOuterR = new boolean[rightTable.getRowCount()];
        for(int i=0;i<this.getRowCount();i++) // leftTable 행 for 문
        {
            for(int j=0;j<rightTable.getRowCount();j++) // rightTable 행 for 문
            {
                boolean check = true;
                for(JoinColumn join : joinColumns)
                {
                    ColumnImpl col1 = (ColumnImpl) this.getColumn(join.getColumnOfThisTable());
                    ColumnImpl col2 = (ColumnImpl) rightTable.getColumn(join.getColumnOfAnotherTable());

                    if(col1.getValue(i)==null || col2.getValue(j)==null) // 비교 값에 null 이 존재할 때
                    {
                        if(col1.getValue(i)!=col2.getValue(j))
                        {
                            check = false;
                            break;
                        }
                    }
                    else if(!(col1.getValue(i).equals(col2.getValue(j))))
                    {
                        check = false;
                        break;
                    }
                }
                if(check==false) continue;
                checkOuterL[i] = true;
                checkOuterR[j] = true;
                columnCount = 0;
                for(int t1=0;t1<this.getColumnCount();t1++)
                    temp[columnCount++].setValue(rowCount,this.getColumn(t1).getValue(i));
                for(int t2=0;t2<rightTable.getColumnCount();t2++)
                    temp[columnCount++].setValue(rowCount,rightTable.getColumn(t2).getValue(j));
                rowCount++;
            }
        }
        System.out.println();
        for(int i=0;i<this.getRowCount();i++)
        {
            if(checkOuterL[i]==false)
            {
                columnCount =0;
                for(int t1=0;t1<this.getColumnCount();t1++)
                    temp[columnCount++].setValue(rowCount,this.getColumn(t1).getValue(i));
                for(int t1=0;t1<rightTable.getColumnCount();t1++) {
                    temp[columnCount++].setValue(rowCount, null);
                }
                rowCount++;
            }
        }
        for(int j=0;j<rightTable.getRowCount();j++)
        {
            if(checkOuterR[j]==false)
            {
                columnCount = 0;
                for(int t1=0;t1<this.getColumnCount();t1++)
                    temp[columnCount++].setValue(rowCount,null);
                for(int t2=0;t2<rightTable.getColumnCount();t2++)
                    temp[columnCount++].setValue(rowCount,rightTable.getColumn(t2).getValue(j));
                rowCount++;
            }
        }
        return new TableImpl(temp);
    }

    @Override
    public String getName() {
        return tableName;
    }

    @Override
    public void show() {
        for(ColumnImpl c:columns) {
            System.out.print(String.format("%"+c.length+"s | ",c.getHeader()));
        }
        System.out.println();
        for(int i=0;i<columns[0].count();i++) {
            for (ColumnImpl c : columns) {
                    System.out.print(String.format("%" + c.length + "s | ", c.getValue(i)));
            }
            System.out.println();
        }
    }

    @Override
    public void describe() {
        int countInt = 0;
        int countString = 0;
        System.out.println(this);
        System.out.println("RangeIndex: "+getRowCount()+" entries, 0 to "+(getRowCount()-1));
        System.out.println("Data columns (total "+getColumnCount()+" columns):");
        ColumnImpl[] tempCol = new ColumnImpl[4];
        tempCol[0] = new ColumnImpl("#");
        tempCol[1] = new ColumnImpl("Column");
        tempCol[2] = new ColumnImpl("Non-Null Count");
        tempCol[3] = new ColumnImpl("Dtype");
        for(int i=0;i<columns.length;i++)
            tempCol[0].setValue(i,i);
        for(int i=0;i<columns.length;i++)
            tempCol[1].setValue(i,columns[i].getHeader());
        for(int i=0;i<columns.length;i++)
            tempCol[2].setValue(i,columns[0].count()-columns[i].getNullCount()+" non-null");
        for(int i=0;i<columns.length;i++) {
            if(columns[i].checkString)
                {
                tempCol[3].setValue(i,"String");
                countString++;
                }
                else
                {
                tempCol[3].setValue(i,"int");
                countInt++;
                }
            }
        Table tempT = new TableImpl(tempCol);
        tempT.show();
        System.out.println("dtypes: int("+countInt+"), String("+countString+")");
        }


    @Override
    public Table head()
    {
        return head(5);
    }

    @Override
    public Table head(int lineCount)
    {
        ColumnImpl[] temp = newColumn();
        lineCount = lineCount > columns[0].count() ? columns[0].count() : lineCount;
        for(int i = 0;i<lineCount;i++)
        {
            for(int j = 0 ;j<columns.length;j++)
                temp[j].setValue(i,columns[j].getValue(i));
        }
        return new TableImpl(temp);
    }

    @Override
    public Table tail() {
        return tail(5);
    }

    @Override
    public Table tail(int lineCount)
    {
        ColumnImpl[] temp = newColumn();
        int count = columns[0].count() - lineCount;
        count = count > 0 ? count : 0;
        for(int i = count; i < columns[0].count(); i++) // 행 개수
        {
            for(int j = 0 ;j <columns.length; j++) // 열 개수
                temp[j].setValue(i-count,columns[j].getValue(i));
        }
        return new TableImpl(temp);
    }

    @Override
    public Table selectRows(int beginIndex, int endIndex)
    {
        ColumnImpl[] temp = newColumn();
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
        return new TableImpl(temp);
    }

    @Override
    public Table selectRowsAt(int... indices)
    {
        int count = 0;
        ColumnImpl[] temp = newColumn();
        for(int a : indices)
        {
            if(a<0 || a>columns[0].count()) {System.out.println(a+": 존재하지 않는 index"); continue;}
            for(int i=0;i<columns.length;i++)
                temp[i].setValue(count,columns[i].getValue(a));
            count++;
        }
        return new TableImpl(temp);
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
        ColumnImpl[] temp = new ColumnImpl[endIndex-beginIndex];
        int count = 0;
        for(int i=beginIndex;i<endIndex;i++)
        {
            temp[count++] = new ColumnImpl(columns[i].getHeader());
        }
        for(int i=0;i<columns[0].count();i++)
        {
            for(int j=beginIndex;j<endIndex;j++)
            {
                temp[j-beginIndex].setValue(i,columns[j].getValue(i));
            }
        }
        return new TableImpl(temp);
    }

    @Override
    public Table selectColumnsAt(int... indices) {
        ColumnImpl[] temp = new ColumnImpl[indices.length];
        int count = 0;
        for(int a : indices)
        {
            if(a<0 || a>columns.length) {System.out.println(indices+": 존재하지 않는 index"); break;}
            temp[count] = new ColumnImpl(columns[a].getHeader());
            for(int i=0;i<columns[0].count();i++)
                temp[count].setValue(i,columns[a].getValue(i));
            count++;
        }
        return new TableImpl(temp);
    }

    @Override
        public <T> Table selectRowsBy(String columnName, Predicate<T> predicate)
    {
        int count = 0;
        ColumnImpl col = (ColumnImpl) this.getColumn(columnName);
        ColumnImpl[] temp = newColumn();
        for(int i=0;i<col.count();i++)
        {
            try{
            if(predicate.test((T)col.getValue(i)))
                {
                for(int j = 0 ;j<columns.length;j++)
                    temp[j].setValue(count,columns[j].getValue(i));
                count++;
                }
            }
            catch(ClassCastException e)
            {
                Integer tmpInt = Integer.parseInt(col.getValue(i));
                if(predicate.test((T) tmpInt))
                {
                    for(int j = 0 ;j<columns.length;j++)
                        temp[j].setValue(count,columns[j].getValue(i));
                    count++;
                }
            }
        }
        return new TableImpl(temp);
    }

    @Override
    public Table sort(int byIndexOfColumn, boolean isAscending, boolean isNullFirst)
    {
        if(byIndexOfColumn>=getColumnCount())
        {
            System.out.println("존재하지 않는 index입니다 원본 Table을 반환합니다.");
            return this;
        }
        ColumnImpl col = columns[byIndexOfColumn];
        ArrayList a = new ArrayList(columns[byIndexOfColumn].list);
            if (isAscending) {
                if (isNullFirst) a.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
                else a.sort(Comparator.nullsLast(Comparator.naturalOrder()));
            } else {
                if (isNullFirst) a.sort(Comparator.nullsFirst(Comparator.reverseOrder()));
                else a.sort(Comparator.nullsLast(Comparator.reverseOrder()));
            }
        boolean[] check = new boolean[columns[0].count()];
        ColumnImpl[] temp = newColumn();
        int count = 0;
        for(var b : a)
        {
            for(int i=0;i<columns[0].count();i++)
            {
                if(b==null)
                {
                    if(col.getValue(i)==null&&!(check[i])){
                            check[i] = true;
                            for (int j = 0; j < columns.length; j++) {
                                temp[j].setValue(count, columns[j].getValue(i));
                            }
                            count++;
                            break;
                        }
                }
                else if(col.checkString)
                {
                    if(b.equals(col.getValue(i)))
                    {
                        check[i]= true;
                        for(int j=0;j<columns.length;j++)
                        {
                            temp[j].setValue(count,columns[j].getValue(i));
                        }
                        count++;
                        break;
                    }
                }
                else
                {
                    if(b.toString().equals(col.getValue(i)))
                    {
                        check[i]= true;
                        for(int j=0;j<columns.length;j++)
                        {
                            temp[j].setValue(count,columns[j].getValue(i));
                        }
                        count++;
                        break;
                    }
                }
            }
        }
        columns = temp;
        return this;
    }

    @Override
    public int getRowCount() {
        return columns[0].count();
    }

    @Override
    public int getColumnCount() {
        return columns.length;
    }

    @Override
    public Column getColumn(int index) {
        return columns[index];
    }

    @Override
    public Column getColumn(String name) {
        for(ColumnImpl c:columns)
        {
            if(name.equals(c.getHeader())) return c;
        }
        return null;
    }
    ColumnImpl[] newColumn()
    {
        ColumnImpl[] temp = new ColumnImpl[columns.length];
        for(int i = 0;i<columns.length;i++)
            temp[i] = new ColumnImpl(columns[i].getHeader());
        return temp;
    }
}
