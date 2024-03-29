package database;

import java.io.*;
import java.util.*;

public class Database {
    // 테이블명이 같으면 같은 테이블로 간주된다.
    private static final Set<Table> tables = new HashSet<>();


    // 테이블 이름 목록을 출력한다.
    public static void showTables() {
        Table []tempTables = new Table[tables.size()];
        tables.toArray(tempTables);
        for(Table e: tempTables)
        {
            System.out.println(e.getName());
        }

    }

    /**
     * 파일로부터 테이블을 생성하고 table에 추가한다.
     *
     * @param csv 확장자는 csv로 가정한다.
     *            파일명이 테이블명이 된다.
     *            csv 파일의 1행은 컬럼명으로 사용한다.
     *            csv 파일의 컬럼명은 중복되지 않는다고 가정한다.
     *            컬럼의 데이터 타입은 int 아니면 String으로 판정한다.
     *            String 타입의 데이터는 ("), ('), (,)는 포함하지 않는 것으로 가정한다.
     */
    public static void createTable(File csv) throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader(csv));
        String[] oneLine = br.readLine().split(",");
        int rowNum = oneLine.length;
        int count = 0;
        ColumnImpl[] tempCol = new ColumnImpl[rowNum];
        for(int i=0;i<rowNum;i++)
        {
            tempCol[i] = new ColumnImpl(oneLine[i]);
        }
        try {
            while (true) {
                oneLine = br.readLine().split(",");
                for (int i = 0; i < rowNum; i++) { // rowNum = 6
                    if(i >= oneLine.length) // length = 5
                    {
                        tempCol[i].setValue(count,null);
                    }
                    else
                    {
                        tempCol[i].setValue(count,oneLine[i]);
                    }
                }
                count++;
            }
        }
        catch(NullPointerException e)
        {
        }
        finally
        {
            tables.add(new TableImpl(csv.getName(),tempCol));
        }
    }

    // tableName과 테이블명이 같은 테이블을 리턴한다. 없으면 null 리턴.
    public static Table getTable(String tableName) {
        Iterator iter = tables.iterator();
        while(iter.hasNext())
        {
            Table temp = (TableImpl)iter.next();
            if(temp.getName().equals(tableName))
                return temp;
        }
        return null;
    }

    /**
     * @return 정렬된 새로운 Table 객체를 반환한다. 즉, 첫 번째 매개변수 Table은 변경되지 않는다.
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생시켜도 됨.
     */
    public static Table sort(Table table, int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        if(byIndexOfColumn>=table.getColumnCount())
        {
            System.out.println("존재하지 않는 index입니다 null을 반환합니다.");
            return null;
        }
        ColumnImpl col = (ColumnImpl)table.getColumn(byIndexOfColumn);
        ArrayList a = new ArrayList(col.list);
        if (isAscending) {
            if (isNullFirst) a.sort(Comparator.nullsFirst(Comparator.naturalOrder()));
            else a.sort(Comparator.nullsLast(Comparator.naturalOrder()));
        } else {
            if (isNullFirst) a.sort(Comparator.nullsFirst(Comparator.reverseOrder()));
            else a.sort(Comparator.nullsLast(Comparator.reverseOrder()));
        }
        boolean[] check = new boolean[col.count()];
        ColumnImpl[] temp = new ColumnImpl[table.getColumnCount()];
        for(int i = 0;i<table.getColumnCount();i++)
            temp[i] = new ColumnImpl(table.getColumn(i).getHeader());
        int count = 0;
        for(var b : a)
        {
            for(int i=0;i<table.getRowCount();i++)
            {
                if(b==null)
                {
                    if(col.getValue(i)==null&&!(check[i])){
                        check[i] = true;
                        for (int j = 0; j < table.getColumnCount(); j++) {
                            temp[j].setValue(count, table.getColumn(j).getValue(i));
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
                        for(int j=0;j< table.getColumnCount();j++)
                        {
                            temp[j].setValue(count,table.getColumn(j).getValue(i));
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
                        for(int j=0;j<table.getColumnCount();j++)
                        {
                            temp[j].setValue(count,table.getColumn(j).getValue(i));
                        }
                        count++;
                        break;
                    }
                }
            }
        }
        return new TableImpl(temp);
    }
}
