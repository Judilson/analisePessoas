/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.gex;

import br.com.gex.conexao.ConnectionMongo;
import br.com.gex.conexao.ConnectionOracle;
import com.mongodb.DB;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jjunior
 */
public class TesteConexao {

    public static void main(String[] argv) {

        DB db = ConnectionMongo.getInstance().getClient(ConnectionMongo.ENDERECOESCRITA, ConnectionMongo.PORTAESCRITA, null, null).getDB("giex");

        db.getStats();

        Connection conn = new ConnectionOracle().conecta();


    }
}
