/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.gex;

import br.com.gex.conexao.ConnectionOracle;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jjunior
 */
public class TesteConexao {

    static ResultSet resultSetPess = null;

    public static void main(String[] argv) {

        try {

            Connection conn = new ConnectionOracle().conecta();
            PreparedStatement psmtPessoa = null;

            String queryPessoa = "select pess_id_pessoa, cred_id_credor from giexbase.tb_pessoas_indicadores"
                    + " where pein_st_lote = 'S'";

            psmtPessoa = conn.prepareStatement(queryPessoa);
            //psmt.setInt(1, Integer.parseInt(request.getParameter("idCredor")));

            resultSetPess = psmtPessoa.executeQuery();

            ExecutorService pool = Executors.newFixedThreadPool(5);
            while (resultSetPess.next()) {

                for (int i = 1; i <= 5; i++) {
                    Runnable r = new Runnable() {
                        InsertIndicadorPessoa iip;

                        public void run() {

                            try {
                                iip = new InsertIndicadorPessoa(resultSetPess.getInt("cred_id_credor"), resultSetPess.getInt("pess_id_pessoa"));
                            } catch (SQLException ex) {
                                Logger.getLogger(TesteConexao.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            iip.start();
                        }
                    };
                    pool.execute(r);

                }

            }
            conn.close();
        } catch (NumberFormatException | SQLException e) {
            e.printStackTrace();
        }

    }
}
