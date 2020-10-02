import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataBaseSync {
	Connection mysql_con_send, mysql_con_receive, con;
        DBHandler dbmysql_send, dbmysql_receive, dbmysql;
        public int process_unit, game_details_rows;
        public String[][] data_rows, game_details;
        int process_counter = 0; 
        int loop_counter = 0;
        
        int count_insert_row_in_games_revenue = 0;
        String insert_String = "";
        
        int count_insert_row_games_revenue_receive_dump = 0;
        String insert_mismatch_adder_String = "";
        
        int count_update_adder_success_row_in_games_revenue = 0;
        String update_adder_success_String = "UPDATE student_management_system.games_revenue SET STATUS='S' WHERE ";
        
        String update_adder_failure_String = "UPDATE student_management_system.games_revenue SET STATUS='F' WHERE ";
        
        int count_update_adder_ignore_row_in_games_revenue = 0;
        String update_adder_ignore_String = "UPDATE student_management_system.games_revenue SET STATUS='M' WHERE ";        
        
        HashMap<String, String> raw_data = new HashMap<>();
        
        
	public MMReportSync() {         
            DBHandler.initializer();
            dbmysql_send = new DBHandler();
            dbmysql_send.createMySqlConnection_sender();
            mysql_con_send = dbmysql_send.getDb_con();  

            dbmysql_receive = new DBHandler();
            dbmysql_receive.createMySqlConnection_receiver();
            mysql_con_receive = dbmysql_receive.getDb_con();

            dbmysql = new DBHandler();
            dbmysql.createMySqlConnection_sender();
            con = dbmysql.getDb_con();

            System.out.print("Enter number of process unit: ");
            Scanner input = new Scanner(System.in);
            process_unit = input.nextInt();
            data_rows = new String[process_unit][16];
            
            if(!isRunning()){
                try {
                    String configurationString = "update student_management_system.configuration application_status = 1 where id = 1";
                    Statement configurationStatement = con.createStatement();
                    configurationStatement.execute(configurationString);
                } catch (SQLException ex) {
                    Logger.getLogger(MMReportSync.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            while(isRunning()){
                if(loop_counter==0 || loop_counter==numberOfLoop())
                {
                    get_game_details_row_count();          
                    set_game_details(); 
                }   
                //single_syncing_process();
                batch_syncing_process(); 
            }
            
            System.out.println(insert_String.length());
            insert(insert_String);
            
            if(count_update_adder_success_row_in_games_revenue!=0)
                update("S");
            System.out.println(update_adder_success_String); 
            
            if(count_update_adder_ignore_row_in_games_revenue!=0){
                update("M");
                System.out.println(insert_mismatch_adder_String);
                insert_mismatch(insert_mismatch_adder_String);
            }
            
            System.out.println(update_adder_ignore_String);
            insert_String = "";
	}
        public int numberOfLoop(){
            int numberOfLoop=0;
            try {
                String numberOfLoopString = "select application_status from student_management_system.configuration where id=1";
                Statement numberOfLoopStatement = con.createStatement();
                ResultSet numberOfLoopResultSet = numberOfLoopStatement.executeQuery(numberOfLoopString);               
                if(numberOfLoopResultSet.next()){
                    numberOfLoop = numberOfLoopResultSet.getInt("number_of_loop");
                }
            } catch (SQLException ex) {
                return 50;
            }
            return numberOfLoop;
        }
        public boolean isRunning(){
            int isRunningResult=0;
            try {
                String isRunnString = "select application_status from student_management_system.configuration where id=1";
                Statement isRunninStatement = con.createStatement();
                ResultSet isRunningResultSet = isRunninStatement.executeQuery(isRunnString);               
                if(isRunningResultSet.next()){
                    isRunningResult = isRunningResultSet.getInt("application_status");
                }
            } catch (SQLException ex) {
                Logger.getLogger(MMReportSync.class.getName()).log(Level.SEVERE, null, ex);
            }
            if(isRunningResult==0)
                return false;
            return true;
        }
        public void get_game_details_row_count(){
            try{
                Statement game_details_rows_count_Statement = con.createStatement();
                String game_details_rows_count_String = "SELECT COUNT(*) FROM student_management_system.game_details";
                ResultSet game_details_rows_count_Resultset = game_details_rows_count_Statement.executeQuery(game_details_rows_count_String);
                game_details_rows_count_Resultset.next();
                game_details_rows = game_details_rows_count_Resultset.getInt("COUNT(*)");
                game_details = new String[game_details_rows][5];
            } catch(SQLException se){
               //
            }
        }
        public void insert_adder(HashMap data){
            if(count_insert_row_in_games_revenue == 0){
                insert_String = "INSERT INTO student_management_system.games_revenue_receive (id,"+
                "organization_id,oem,device_type,msisdn,imei,imsi,keyword,game_name,charged_amount,operator,"+
                "game_provider,brand_name,model,related_mo,update_time,action_time)\nvalues";
                count_insert_row_in_games_revenue++;
            }            
            insert_String += "('" + data.get("id") + "','" + data.get("organization_id") + "','" + data.get("oem")
                    + "','" + data.get("device_type") + "','" + data.get("msisdn") + "','" + data.get("imei") 
                    + "','" + data.get("imsi") + "','" + data.get("keyword") + "','" + data.get("game_name") 
                    + "','" + data.get("charged_amount") + "','" + data.get("operator") + "','" + data.get("game_provider") 
                    + "','" + data.get("brand_name") + "','" + data.get("model") + "','" + data.get("related_mo")
                    + "','" + data.get("update_time") + "','" + data.get("action_time")  + "')," ;
            count_insert_row_in_games_revenue++;
        }
        public void insert(String st){
            try{
                Statement insert_Statement = con.createStatement();
                insert_String = insert_String.substring(0,insert_String.length()-1);
                insert_String+=';';
                System.out.println(insert_String);
                insert_Statement.execute(insert_String);
            } catch(Exception se){
                update("M");
            } 
        }
        public void insert_mismatch_adder(HashMap data){
            if(count_insert_row_games_revenue_receive_dump == 0){
                insert_mismatch_adder_String = "INSERT INTO student_management_system.games_revenue_receive_dump (id,"+
                "msisdn,imei,imsi,keyword,game_name,charged_amount,operator,game_provider,brand_name,"+
                "model,related_mo,update_time,action_time)\nvalues";
                count_insert_row_games_revenue_receive_dump++;
            }            
            insert_mismatch_adder_String += "('" + data.get("id") + "','" + data.get("msisdn") + "','" + data.get("imei") 
                    + "','" + data.get("imsi") + "','" + data.get("keyword") + "','" + data.get("game_name") 
                    + "','" + data.get("charged_amount") + "','" + data.get("operator") + "','" + data.get("game_provider") 
                    + "','" + data.get("brand_name") + "','" + data.get("model") + "','" + data.get("related_mo")
                    + "','" + data.get("update_time") + "','" + data.get("action_time")  + "')," ;
            count_insert_row_games_revenue_receive_dump++;
        }
        public void insert_mismatch(String st){
            try{
                Statement insert_Statement = con.createStatement();
                insert_mismatch_adder_String = insert_mismatch_adder_String.substring(0,insert_mismatch_adder_String.length()-1);
                insert_mismatch_adder_String+=';';
                System.out.println(insert_mismatch_adder_String);
                insert_Statement.execute(insert_mismatch_adder_String);
            } catch(Exception se){
                se.printStackTrace();
            } 
        }
        public void update_adder(HashMap data, String to_status){    
            if(to_status=="S"){
                update_adder_success_String += "(id='" + data.get("id") + "' and msisdn='" + data.get("msisdn") +   
                                               "' and imei='" + data.get("imei") + "' and keyword='" + 
                                                data.get("keyword") + "' and game_name='" + data.get("game_name")+
                                               "' and action_time='" + data.get("action_time") + "') OR ";
                count_update_adder_success_row_in_games_revenue++;
            }                          
            else if(to_status=="M"){
                update_adder_ignore_String += "(id='" + data.get("id") + "' and msisdn='" + data.get("msisdn") +   
                                               "' and imei='" + data.get("imei") + "' and keyword='" + 
                                                data.get("keyword") + "' and game_name='" + data.get("game_name")+
                                               "' and action_time='" + data.get("action_time") + "') OR ";
                count_update_adder_ignore_row_in_games_revenue++;
            }                
        }
        public void update(String todo){
            try{
                Statement update_Statement = con.createStatement();
                if(todo=="S"){
                    update_adder_success_String = update_adder_success_String.substring(0,update_adder_success_String.length()-3);
                    update_adder_success_String+=';';
                    update_Statement.execute(update_adder_success_String);
                }
                else if(todo=="F"){
                    update_adder_failure_String = update_adder_failure_String.substring(0,update_adder_failure_String.length()-3);
                    update_adder_failure_String+=';';
                    update_Statement.execute(update_adder_failure_String);
                }
                else if(todo=="M"){
                    update_adder_ignore_String = update_adder_ignore_String.substring(0,update_adder_ignore_String.length()-3);
                    update_adder_ignore_String+=';';
                    update_Statement.execute(update_adder_ignore_String);
                }
            } catch(Exception se){
                se.printStackTrace();
            } 
        }
        
        public void set_game_details(){
            int counter = 0;            
            try{
                Statement game_details_statement = con.createStatement();
                String game_details_select = "SELECT organization_id,oem,keyword,sub_keyword,device_type FROM student_management_system.game_details";
                ResultSet rs_game_details = game_details_statement.executeQuery(game_details_select);
                System.out.println(game_details_select);
                while(rs_game_details.next()){
                    game_details[counter][0] = rs_game_details.getString("organization_id");
                    game_details[counter][1] = rs_game_details.getString("oem");
                    game_details[counter][2] = rs_game_details.getString("keyword");
                    game_details[counter][3] = rs_game_details.getString("sub_keyword");
                    game_details[counter][4] = rs_game_details.getString("device_type");
                    counter++;
                }
            } catch(SQLException se){
                
            }
        }
	
        public void single_syncing_process(){           
            
            System.out.println("Sync Started:");
            try{			
		System.out.println("Result from Mysql to Mysql");
                
                Statement result_from_send = mysql_con_send.createStatement();
		String sql_select_statement = "SELECT * FROM student_management_system.games_revenue WHERE status = 'N' LIMIT " + process_unit;
		ResultSet rs_send = result_from_send.executeQuery(sql_select_statement);
                System.out.println(sql_select_statement);
               		
                boolean flag; 
		while(rs_send.next()){
                    flag = false;
                    raw_data.put("id",rs_send.getString("id"));
                    raw_data.put("msisdn",rs_send.getString("msisdn"));
                    raw_data.put("imei",rs_send.getString("imei"));
                    raw_data.put("imsi",rs_send.getString("imsi"));
                    raw_data.put("keyword",rs_send.getString("keyword"));
                    raw_data.put("game_name",rs_send.getString("game_name"));
                    raw_data.put("charged_amount",rs_send.getString("charged_amount"));
                    raw_data.put("operator",rs_send.getString("operator"));
                    raw_data.put("game_provider",rs_send.getString("game_provider"));
                    raw_data.put("brand_name",rs_send.getString("brand_name"));
                    raw_data.put("model",rs_send.getString("model"));
                    raw_data.put("related_mo",rs_send.getString("related_mo"));
                    raw_data.put("update_time",rs_send.getString("update_time"));
                    raw_data.put("action_time",rs_send.getString("action_time"));
                    raw_data.put("organization_id","null");
                    raw_data.put("oem","null");
                    raw_data.put("device_type","null"); 
                    
                    for(int i=0;i<game_details_rows;i++){
                        if(raw_data.get("keyword").equals(game_details[i][2])){
                            if(raw_data.get("keyword").equals("MMGAMES")){
                                if(raw_data.get("game_name").substring(0,1).equals(game_details[i][3]) || 
                                    raw_data.get("game_name").substring(0,2).equals(game_details[i][3]) ||
                                    raw_data.get("game_name").substring(0,4).equals(game_details[i][3])){
                                    raw_data.put("organization_id",game_details[i][0]);
                                    raw_data.put("oem",game_details[i][1]);
                                    raw_data.put("device_type",game_details[i][4]);
                                    flag=true;
                                    break;
                                }
                            }
                            else if(!raw_data.get("keyword").equals("MMGAMES")){
                                raw_data.put("organization_id",game_details[i][0]);
                                raw_data.put("oem",game_details[i][1]);
                                raw_data.put("device_type",game_details[i][4]);
                                flag=true;
                                break;
                            }
                        }
                    }                    
                    
                    if(flag){
                        try {
                            String insertString = "INSERT INTO student_management_system.games_revenue_receive_dump (id,"+
                            "msisdn,imei,imsi,keyword,game_name,charged_amount,operator,game_provider,brand_name,"+
                            "model,related_mo,update_time,action_time) values('" + raw_data.get("id") + "','" + raw_data.get("msisdn") + "','" + raw_data.get("imei") 
                            + "','" + raw_data.get("imsi") + "','" + raw_data.get("keyword") + "','" + raw_data.get("game_name") 
                            + "','" + raw_data.get("charged_amount") + "','" + raw_data.get("operator") + "','" + raw_data.get("game_provider") 
                            + "','" + raw_data.get("brand_name") + "','" + raw_data.get("model") + "','" + raw_data.get("related_mo")
                            + "','" + raw_data.get("update_time") + "','" + raw_data.get("action_time")  + "')";
                            Statement insertStatement = con.createStatement();
                            insertStatement.executeQuery(insertString);
                            String updateString = "UPDATE student_management_system.games_revenue SET STATUS='S' WHERE (id='" + raw_data.get("id") + "' and msisdn='" + raw_data.get("msisdn") +
                                            "' and imei='" + raw_data.get("imei") + "' and keyword='" + raw_data.get("keyword") + "' and game_name='" + raw_data.get("game_name")+
                                            "' and action_time='" + raw_data.get("action_time") + "')";
                            Statement updateStatement = con.createStatement();
                            updateStatement.executeQuery(updateString);
                        } catch (Exception e) {
                            String updateString = "UPDATE student_management_system.games_revenue SET STATUS='F' WHERE (id='" + raw_data.get("id") + "' and msisdn='" + raw_data.get("msisdn") +
                                            "' and imei='" + raw_data.get("imei") + "' and keyword='" + raw_data.get("keyword") + "' and game_name='" + raw_data.get("game_name")+
                                            "' and action_time='" + raw_data.get("action_time") + "')";
                            Statement updateStatement = con.createStatement();
                            updateStatement.executeQuery(updateString);
                        }              
                    }else{
                        String updateString = "UPDATE student_management_system.games_revenue SET STATUS='M' WHERE (id='" + raw_data.get("id") + "' and msisdn='" + raw_data.get("msisdn") +
                                            "' and imei='" + raw_data.get("imei") + "' and keyword='" + raw_data.get("keyword") + "' and game_name='" + raw_data.get("game_name")+
                                            "' and action_time='" + raw_data.get("action_time") + "')";
                        Statement updateStatement = con.createStatement();
                        updateStatement.executeQuery(updateString);
                        String insert_mismatch_String = "INSERT INTO student_management_system.games_revenue_receive_dump (id,"+
                                "msisdn,imei,imsi,keyword,game_name,charged_amount,operator,game_provider,brand_name,"+
                                "model,related_mo,update_time,action_time)\nvalues('" + raw_data.get("id") + "','" + raw_data.get("msisdn") + "','" + raw_data.get("imei") 
                            + "','" + raw_data.get("imsi") + "','" + raw_data.get("keyword") + "','" + raw_data.get("game_name") 
                            + "','" + raw_data.get("charged_amount") + "','" + raw_data.get("operator") + "','" + raw_data.get("game_provider") 
                            + "','" + raw_data.get("brand_name") + "','" + raw_data.get("model") + "','" + raw_data.get("related_mo")
                            + "','" + raw_data.get("update_time") + "','" + raw_data.get("action_time")  + "')";
                        Statement insertStatement = con.createStatement();
                        insertStatement.executeQuery(insert_mismatch_String);
                        
                    }                        
                }  			
            }catch(SQLException se){
                se.printStackTrace();
            }                  
            System.out.println("Sync Done");
}
	public void batch_syncing_process(){           
            
            System.out.println("Sync Started:");
            try{			
		System.out.println("Result from Mysql to Mysql");
                
                Statement result_from_send = mysql_con_send.createStatement();
		String sql_select_statement = "SELECT * FROM student_management_system.games_revenue WHERE status = 'N' LIMIT " + process_unit;
		ResultSet rs_send = result_from_send.executeQuery(sql_select_statement);
                System.out.println(sql_select_statement);
               		
                boolean flag; 
		while(rs_send.next()){
                    flag = false;
                    raw_data.put("id",rs_send.getString("id"));
                    raw_data.put("msisdn",rs_send.getString("msisdn"));
                    raw_data.put("imei",rs_send.getString("imei"));
                    raw_data.put("imsi",rs_send.getString("imsi"));
                    raw_data.put("keyword",rs_send.getString("keyword"));
                    raw_data.put("game_name",rs_send.getString("game_name"));
                    raw_data.put("charged_amount",rs_send.getString("charged_amount"));
                    raw_data.put("operator",rs_send.getString("operator"));
                    raw_data.put("game_provider",rs_send.getString("game_provider"));
                    raw_data.put("brand_name",rs_send.getString("brand_name"));
                    raw_data.put("model",rs_send.getString("model"));
                    raw_data.put("related_mo",rs_send.getString("related_mo"));
                    raw_data.put("update_time",rs_send.getString("update_time"));
                    raw_data.put("action_time",rs_send.getString("action_time"));
                    raw_data.put("organization_id","null");
                    raw_data.put("oem","null");
                    raw_data.put("device_type","null"); 
                    
                    for(int i=0;i<game_details_rows;i++){
                        if(raw_data.get("keyword").equals(game_details[i][2])){
                            if(raw_data.get("keyword").equals("MMGAMES")){
                                if(raw_data.get("game_name").substring(0,1).equals(game_details[i][3]) || 
                                    raw_data.get("game_name").substring(0,2).equals(game_details[i][3]) ||
                                    raw_data.get("game_name").substring(0,4).equals(game_details[i][3])){
                                    raw_data.put("organization_id",game_details[i][0]);
                                    raw_data.put("oem",game_details[i][1]);
                                    raw_data.put("device_type",game_details[i][4]);
                                    flag=true;
                                    break;
                                }
                            }
                            else if(!raw_data.get("keyword").equals("MMGAMES")){
                                raw_data.put("organization_id",game_details[i][0]);
                                raw_data.put("oem",game_details[i][1]);
                                raw_data.put("device_type",game_details[i][4]);
                                flag=true;
                                break;
                            }
                        }
                    }                    
                    
                    if(flag){
                        update_adder(raw_data,"S");
                        update_adder(raw_data,"F");    //incase of failure
                        insert_adder(raw_data);
                    }else{
                        update_adder(raw_data,"M");
                        insert_mismatch_adder(raw_data);
                    }                        
                }  			
            }catch(SQLException se){
                se.printStackTrace();
            }                  
            System.out.println("Sync Done");
}
    public static void main(String[] args) {
            MMReportSync sync=new MMReportSync();
    }
}

