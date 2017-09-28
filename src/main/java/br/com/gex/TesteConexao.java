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

/**
 *
 * @author jjunior
 */
public class TesteConexao {

    public static void main(String[] argv) {

        try {

            Connection conn = new ConnectionOracle().conecta();
            PreparedStatement psmtPessoa = null;

            String queryPessoa = "select pess_id_pessoa, cred_id_credor from giexbase.tb_pessoas_indicadores"
                    + " where pein_st_lote = 'S'";

            psmtPessoa = conn.prepareStatement(queryPessoa);
            //psmt.setInt(1, Integer.parseInt(request.getParameter("idCredor")));

            ResultSet resultSetPess = psmtPessoa.executeQuery();

            while (resultSetPess.next()) {

                new InsertIndicadorPessoa().insert(resultSetPess.getInt("pess_id_pessoa"), resultSetPess.getInt("cred_id_credor"));
                
            }

            conn.close();
        } catch (NumberFormatException | SQLException e) {
            e.printStackTrace();
        }

    }
}
