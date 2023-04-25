package superheroes;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SuperheroesDatabase {
	private Connection conn = null;

	public SuperheroesDatabase() {

	}
	
	/**
	 * 
	 * @return true si abre correctamente la conexion
	 * y false si ninguna conexion es abierta
	 */

	public boolean openConnection() {
		String serverAddress = "127.0.0.1:3306";
		String db = "superheroes";
		String user = "superheroes_user";
		String pass = "superheroes_pass";
		String url = "jdbc:mysql://" + serverAddress + "/" + db;
		Boolean res = false;
		try {
			if (conn==null || conn.isClosed()) {
				conn = DriverManager.getConnection(url, user, pass);
				res = true;
			}
			System.out.println("Conectado a la base de datos");
		} catch (SQLException e) {
			System.out.println("No se ha podido conectar a la base de datos");
		}
		return res;
	}
	/**
	 * 
	 * @return true si se ejecuta sin errores y
	 * false si ocurre una excepción
	 */
	public boolean closeConnection() {
		boolean res = false;
		try {
			if(conn == null) return true;
			if(conn.isClosed()) return true;
			conn.close();
			res = true;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}
	/**
	 * Implementa la creacion de la tabla escena
	 * @return false si la tabla no se puede crear y
	 * true si la tabla se ha creado correctamente
	 */
	public boolean createTableEscena() {
		openConnection();
		boolean result = true;
		String query = "CREATE TABLE escena (" + " id_pelicula INT, " + 
				"  n_orden INT," + "titulo VARCHAR(100)," + " duracion INT," +
				"  primary key (n_orden, id_pelicula)," + "foreign key (id_pelicula) references pelicula (id_pelicula) on delete cascade on update cascade" + ");";
		try{
			DatabaseMetaData dbMet = conn.getMetaData();
			ResultSet rs = dbMet.getTables(null, "superheroes", "escena", null);
			if(!rs.next()) {
				Statement st = conn.createStatement ();
				st.executeUpdate(query);
				System.out.println("Tabla escena creada correctamente");
			}
			else {
				result = false;
				System.out.println("Ya existe la tabla escena");
			}

		}catch(SQLException e){
			System.out.println("Error al crear tabla escena");
			result = false;
		}
		return result;
	}
	
	/**
	 * Implementa la creación de la tabla rival
	 * @return false si la tabla no se ha podido crear y
	 * true si la tabla se ha creado correctamente
	 */
	
	public boolean createTableRival() {
		openConnection();
		boolean result = true;
		String query = "CREATE TABLE rival (" + " id_sup INT, " + 
				" id_villano INT," + " fecha_primer_encuentro DATE ,"  +
				"  primary key (id_villano, id_sup)," + "foreign key (id_villano) references villano (id_villano) on delete cascade on update cascade," + 
				"foreign key (id_sup) references superheroe (id_sup) on delete cascade on update cascade" + ");";
		try{
			DatabaseMetaData dbMet = conn.getMetaData();
			ResultSet rs = dbMet.getTables(null, "superheroes", "rival", null);
			if(!rs.next()) {
				Statement st = conn.createStatement ();
				st.executeUpdate(query);
				System.out.println("Tabla rival creada correctamente");
			}
			else {
				result = false;
				System.out.println("Ya existe la tabla rival");
			}
		}catch(SQLException e){
			System.out.println("Error al crear tabla rival");
			result = false;
		}
		return result;
	}
	
	/**
	 * Inserta en la base de datos las escenas contenidas en el fichero que se pasa como parametro
	 * @param fileName
	 * @return <int> la cantidad de elementos insertados en la tabla
	 */
	public int loadEscenas(String fileName) {
		openConnection();
		int res = 0;
		String query = "INSERT INTO escena (id_pelicula, n_orden, titulo, duracion) Values(?,?,?,?)";
		BufferedReader br;
		try {
			br = new  BufferedReader(new FileReader(fileName));
			String line = br.readLine();
			while (line != null) {
				String [] fields = line.split(";");
				String id_pelicula = fields[0];
				String n_orden = fields[1];
				String titulo = fields[2];
				String duracion = fields[3];
				try{
					PreparedStatement pst = conn.prepareStatement(query);
					pst.setString(1, id_pelicula);
					pst.setString(2, n_orden);
					pst.setString(3, titulo);
					pst.setString(4, duracion);
					res += pst.executeUpdate();
				}
				catch (SQLException esql) {
					System.err.println("Mensaje: " + esql.getMessage());
					System.err.println("Código: " + esql.getErrorCode());
					System.err.println("Estado SQL: " + esql.getSQLState());
				}
				line = br.readLine();
			}


		}catch (FileNotFoundException e1) {
			System.out.println("Archivo no encontrado");
			e1.printStackTrace();
		}
		catch(IOException e) {
			System.out.println("Error en el metodo readLine");
		}


		return res;
	}
	
	/**
	 * Inserta en la base de datos los datos sobre qué superhéroes con qué villanos
	 * protagonizan qué películas, que están en el fichero que se pasa como parámetro
	 * @param fileName
	 * @return <int> la cantidad de elementos insertados
	 */
	public int loadProtagoniza(String fileName) {
		openConnection();
		int res = 0;
		String query = "INSERT INTO protagoniza (id_sup, id_villano, id_pelicula) Values(?,?,?)";
		String query2 = "INSERT INTO rival (id_sup, id_villano, fecha_primer_encuentro) Values(?,?,?)";
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		try {
			BufferedReader br = new  BufferedReader(new FileReader(fileName));
			String line = br.readLine();
			while (null!=line) {
				int fila;
				String [] fields = line.split(";");
				String id_sup = fields[0];
				String id_villano = fields[1];
				String id_pelicula = fields[2];
				try {
					PreparedStatement pst = conn.prepareStatement(query);
					pst.setString(1, id_sup);
					pst.setString(2, id_villano);
					pst.setString(3, id_pelicula);
					res += pst.executeUpdate();
				}catch (SQLException esql) {
					System.err.println("Mensaje: " + esql.getMessage());
					System.err.println("Código: " + esql.getErrorCode());
					System.err.println("Estado SQL: " + esql.getSQLState());
					try {
						conn.rollback();
					}catch (SQLException e) {
						e.printStackTrace();
					}
					return 0;
				}
				try{
					PreparedStatement pst2 = conn.prepareStatement(query2);
					pst2.setString(1, id_sup);
					pst2.setString(2, id_villano);
					pst2.setDate(3, new Date(new java.util.Date().getTime()));
					res += pst2.executeUpdate();
				}catch (SQLException esql) {
					System.err.println("Mensaje: " + esql.getMessage());
					System.err.println("Código: " + esql.getErrorCode());
					System.err.println("Estado SQL: " + esql.getSQLState());
				}	
				line = br.readLine();
			}	
		}
		catch(IOException e) {
		}
		try {
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return res;
	}


	/**
	 * Consulta en la base de datos la lista de todas las películas y retorna dicha
	 * lista como String.
	 * @return <String> de todas las peliculas ordenado alfabéticamente.
	 * Si no hay películas almacenadas debe retornarse {}
	 */
	public String catalogo() {
		openConnection();
		String query = "SELECT pelicula.titulo FROM pelicula order by titulo ASC;";
		String listaPeliculas = "{";
		try {
			Statement st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
			if(rs.next()) {
				listaPeliculas = listaPeliculas + rs.getString("titulo");
			}
			while (rs.next()){
				listaPeliculas = listaPeliculas+ ", " + rs.getString("titulo") ;
			}

		} catch (SQLException esql) {
			System.err.println("Mensaje: " + esql.getMessage());
			System.err.println("Código: " + esql.getErrorCode());
			System.err.println("Estado SQL: " + esql.getSQLState());
			return null; 
		}
		
		return listaPeliculas + "}";
	}

	/**
	 * 
	 * @param nombrePelicula
	 * @return <int> la duracion de una pelicula cuyo nombre se da por parámetro
	 * Si no hay ninguna película con ese nombre, debe retornarse -1.0
	 * Si se produce alguna excepción, debe retornarse -2.0. Si la
	 * película existe, pero no hay escenas de ella, debe retornarse 0.0.
	 */
	public int duracionPelicula(String nombrePelicula) {
		openConnection();
		int duracion = 0;
		String query = "SELECT SUM(escena.duracion) as suma FROM superheroes.escena, superheroes.pelicula "
				+ " WHERE pelicula.id_pelicula = escena.id_pelicula AND pelicula.titulo = ?" ;
		String query2 = "SELECT pelicula.titulo FROM pelicula where pelicula.titulo = ?";
		try {
			PreparedStatement pst = conn.prepareStatement(query);
			PreparedStatement pst2 = conn.prepareStatement(query2);
			pst.setString(1, nombrePelicula);
			pst2.setString(1, nombrePelicula);
			ResultSet rs = pst.executeQuery();
			ResultSet rs2 = pst2.executeQuery();
			if(rs2.next()) {
				if(rs.next())
					duracion = rs.getInt("suma");
			}
			else
				duracion = -1;

		}catch (SQLException esql) {
			System.err.println("Mensaje: " + esql.getMessage());
			System.err.println("Código: " + esql.getErrorCode());
			System.err.println("Estado SQL: " + esql.getSQLState());
			return -2;
		}
		return duracion;
	}

	/**
	 * 
	 *
	 * @param nombreVillano
	 * @return <String> la lista de nombres de las escenas de todas las películas
	 * en las que aparezca el villano cuyo nombre se pasa como parámetro. 
	 * Si el villano no existe en la base de datos, o no hay películas
	 * para ese villano, o no haya escenas para sus películas se debe
	 * retornar {}. Si hay alguna excepción devuelve null
	 */
	public String getEscenas(String nombreVillano) {
		openConnection();
		String query = "SELECT escena.titulo FROM superheroes.pelicula, superheroes.escena, superheroes.villano, superheroes.protagoniza"
				+ " WHERE pelicula.id_pelicula = escena.id_pelicula AND pelicula.id_pelicula = protagoniza.id_pelicula"
				+ " AND protagoniza.id_villano = villano.id_villano"
				+ " AND villano.nombre LIKE ? group by escena.titulo order by pelicula.titulo ASC, escena.n_orden ASC;";
		String listaEscenas = "{";
		
		try {
			PreparedStatement pst = conn.prepareStatement(query);
			pst.setString(1, nombreVillano);
			ResultSet rs = pst.executeQuery();
			if(rs.next()) {
				listaEscenas = listaEscenas + rs.getString("titulo");
			}
			while (rs.next()){
				listaEscenas = listaEscenas + ", " + rs.getString("titulo");
			}
		}catch (SQLException esql) {
			System.err.println("Mensaje: " + esql.getMessage());
			System.err.println("Código: " + esql.getErrorCode());
			System.err.println("Estado SQL: " + esql.getSQLState());
			return null;
		}
		return listaEscenas + "}";
	}
	
	/**
	 * 
	 * @param nombre
	 * @param apellido
	 * @param filename
	 * @return <true> si la imagen existía en la base de datos
	 * y se ha almacenado correctamente y <false> en caso contrario.
	 */

	public boolean desenmascara(String nombre, String apellido, String filename) {
		openConnection();
		String query = "SELECT superheroe.avatar FROM superheroes.superheroe, superheroes.persona_real"
				+ " WHERE superheroe.id_persona = persona_real.id_persona AND persona_real.nombre LIKE ? AND persona_real.apellido LIKE ?";
		boolean res = false;
		try {
			PreparedStatement pst = conn.prepareStatement(query);
			pst.setString(1, nombre);
			pst.setString(2, apellido);
			ResultSet rs = pst.executeQuery();
			Blob blob = null;
			byte[] data = null;
			FileOutputStream image = new FileOutputStream(filename);
			if(rs.next()) {
				blob = rs.getBlob("avatar");
				if(blob != null) {
					data = blob.getBytes(1, (int)blob.length());
					image.write(data);
					res = true;
				}
			}
		}catch(SQLException esql) {
			System.err.println("Mensaje: " + esql.getMessage());
			System.err.println("Código: " + esql.getErrorCode());
			System.err.println("Estado SQL: " + esql.getSQLState());
			return false;
		}catch (FileNotFoundException e) {
			System.out.println("Error en el fichero image");
		}catch(IOException e) {
			System.out.println("Error en método write");
		}
		
		return res;
	}

}