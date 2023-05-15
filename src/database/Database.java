package database;

import java.io.*;
import java.util.*;

public class Database {
    static int countTable = 0;
    static ArrayList<Table> table = new ArrayList<>();
    // 테이블명이 같으면 같은 테이블로 간주된다.

    // 테이블 이름 목록을 출력한다.
    public static void showTables() {
        for(Table t:table)
        {
            System.out.println(t.getName());
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
    public static void createTable(File csv) throws IOException,FileNotFoundException {
        int count = 1;
        BufferedReader br = new BufferedReader(new FileReader(csv));
        String[] tempStr = br.readLine().split(","); // 한 줄 받기
        int rowNum = tempStr.length; // 열 개수 저장
        Columnimpl[] tempColumn = new Columnimpl[rowNum]; // 열 개수만큼 Column 생성
        for(int i=0;i<rowNum;i++) {
            tempColumn[i] = new Columnimpl();
           tempColumn[i].setValue(0, tempStr[i]);
        }
        // 여기까지하면 Column에 맨 위에있는 이름을 부여함
        try
        {
            while(true)
            {
                tempStr = br.readLine().split(","); // 마지막 줄 다음줄 받을 때 NPE 발생
                for(int i=0;i<rowNum;i++) // 행 개수만큼 입력
                {
                    if(tempStr.length<=i) {
                        tempColumn[i].setValue(count, null);
                    }
                    else
                    {
                        tempColumn[i].setValue(count,tempStr[i]);
                    }
                }
                count ++;
            }
        }
        catch(NullPointerException e)
        {

        }
        finally
        {
            table.add(new Tableimpl(csv.getName(),tempColumn)); // table에 이름 부여, rowNum만큼 Column 객체 생성 -> 이거 나중에 고칠수도
        }
      countTable++;
    }

    // tableName과 테이블명이 같은 테이블을 리턴한다. 없으면 null 리턴.
    public static Table getTable(String tableName) {
        for(Table t : table)
        {
            if(t.getName().equals(tableName))
                return t;
        }
        return null;
    }

    /**
     * @return 정렬된 새로운 Table 객체를 반환한다. 즉, 첫 번째 매개변수 Table은 변경되지 않는다.
     * @param byIndexOfColumn 정렬 기준 컬럼, 존재하지 않는 컬럼 인덱스 전달시 예외 발생시켜도 됨.
     */
    public static Table sort(Table table, int byIndexOfColumn, boolean isAscending, boolean isNullFirst) {
        return null;
    }
}
