/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.gex.conexao;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 *
 * @author jjunior
 */
public class ConnectionOracle {

    public Connection conecta() {

        Connection conn = null;
        try {
            Class.forName("oracle.jdbc.OracleDriver");
            conn = (Connection) DriverManager.getConnection("jdbc:oracle:thin:@giex-scan.intra.local:1521/TAFGIEXPROD", "giexadmin", "?806y*admin");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Erro ao tentar se conectar ao banco");
        }
        return conn;
    }
}
