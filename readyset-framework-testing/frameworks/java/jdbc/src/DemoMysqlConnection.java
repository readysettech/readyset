import java.sql.*;

class DemoMysqlConnection {
	public static void main(String args[]) {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");

			String vtUsername = System.getenv("RS_USERNAME");
			String vtPassword = System.getenv("RS_PASSWORD");
			String vtHost = System.getenv("RS_HOST");
			String vtPort = System.getenv("RS_PORT");
			String vtDatabase = System.getenv("RS_DATABASE");

			String connectionUri = "jdbc:mysql://" + vtHost + ":" + vtPort + "/" + vtDatabase + "?serverTimezone=UTC";
			System.out.println("+++ URL: " + connectionUri);
			Connection con = DriverManager.getConnection(connectionUri, vtUsername, vtPassword);

			simpleExample(con);
			sysSchemaExample(con, vtDatabase);
			con.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void simpleExample(Connection con) throws SQLException {
		String createTableSql = "CREATE TABLE people (id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL) ";
		String insertUser1Sql = "INSERT INTO people VALUES (1, 'ReadySet User 1')";
		String insertUser2Sql = "INSERT INTO people VALUES (2, 'ReadySet User 2')";
		String insertUser3Sql = "INSERT INTO people VALUES (3, 'ReadySet User 3')";
		String selectPeopleSql = "SELECT COUNT(*) FROM people";
		String updatePeopleSql = "UPDATE people SET name='ReadySet User 500' where id=2";
		String dropPeopleSql = "DROP TABLE people";

		Statement st = con.createStatement();
		st.getConnection();

		// Create table
		st.executeUpdate(createTableSql);

		// Insert three records into people table
		st.executeUpdate(insertUser1Sql);
		st.executeUpdate(insertUser2Sql);
		st.executeUpdate(insertUser3Sql);

		// Select * from people
		ResultSet rs = st.executeQuery(selectPeopleSql);
		rs.next();
		int selectedCount = rs.getInt(1);
		assert selectedCount == 3;

		// Update records
		st.executeUpdate(updatePeopleSql);

		// Select * again
		st.executeQuery(selectPeopleSql);

		// Drop the table
		st.getConnection();
		st.executeUpdate(dropPeopleSql);

		st.close();
	}

	private static void sysSchemaExample(Connection con, String vtDatabase) throws SQLException {
		String createTableSql = "CREATE TABLE `a` (`one` int NOT NULL,`two` int NOT NULL,PRIMARY KEY(`one`, `two`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
		String selectSql = "SELECT column_name column_name, " +
		        "data_type data_type, " +
		        "column_type full_data_type, " +
		        "character_maximum_length character_maximum_length, " +
		        "numeric_precision numeric_precision, " +
		        "numeric_scale numeric_scale," +
		        "datetime_precision datetime_precision," +
		        "column_default column_default," +
		        "is_nullable is_nullable," +
		        "extra extra," +
		        "table_name table_name" +
		        " FROM information_schema.columns WHERE table_schema = ? ORDER BY ordinal_position";
		String dropTableSql = "DROP TABLE `a`";

		PreparedStatement st = con.prepareStatement(selectSql);

		// Create table
		st.executeUpdate(createTableSql);

		System.out.println("select query: " + selectSql);
		st.setString(1, vtDatabase);
		ResultSet rs = st.executeQuery();

		while(rs.next()) {
			assert (rs.getString(1).equals("one")
			        || rs.getString(1).equals("two"));
			assert (rs.getString(2).equals("int"));
			assert (rs.getString(3).equals("int"));
			assert (rs.getString(4).equals(""));
			assert (rs.getString(5).equals("10"));
			assert (rs.getString(6).equals("0"));
			assert (rs.getString(7).equals(""));
			assert (rs.getString(8).equals(""));
			assert (rs.getString(9).equals("NO"));
			assert (rs.getString(10).equals(""));
			assert (rs.getString(11).equals("a"));
		}
		rs.close();

		// Drop table
		st.executeUpdate(dropTableSql);

	}
}
