/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.com.gex;

import br.com.gex.conexao.ConnectionMongo;
import br.com.gex.conexao.ConnectionOracle;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jjunior
 */
public class TesteConexao {

    public static void main(String[] argv) {

        DB db = ConnectionMongo.getInstance().getClient(ConnectionMongo.ENDERECOESCRITA, ConnectionMongo.PORTAESCRITA, null, null).getDB("GIEXONLINE");

        DBCollection collection = db.getCollection("INDICADOR_PESSOAS");

        try {

            Connection conn = new ConnectionOracle().conecta();
            PreparedStatement psmtPessoa = null;
            PreparedStatement psmtCadastro = null;
            PreparedStatement psmtLancamentos = null;

            String queryPessoa = "select pess_id_pessoa, cred_id_credor from giexbase.tb_pessoas_indicadores"
                    + " where pein_st_lote = 'S' and rownum < 10";

            String queryCadastro = "select cada_id_cadastro from giexbase.tb_cadastros_pessoas "
                    + " where pess_id_pessoa = ?";

            String queryLancamentos = "SELECT   li.lanc_id_lancamento,"
                    + "         laid_st_suspenso,"
                    + "         laid_st_prescrito,"
                    + "         laid_st_aberto,"
                    + "         laid_st_cancelado,"
                    + "         laid_st_pago"
                    + "  FROM       giexbase.tb_lancamentos_indicadores li"
                    + "         INNER JOIN"
                    + "             giexbase.tb_lancamentos l"
                    + "         ON li.lanc_id_lancamento = l.lanc_id_lancamento"
                    + " WHERE   l.cred_id_credor = ?"
                    + "         AND l.cada_id_cadastro IN (SELECT   cada_id_cadastro"
                    + "                                      FROM   giexbase.tb_cadastros_pessoas"
                    + "                                     WHERE   pess_id_pessoa = ?)";
            psmtPessoa = conn.prepareStatement(queryPessoa);
            //psmt.setInt(1, Integer.parseInt(request.getParameter("idCredor")));

            ResultSet resultSetPess = psmtPessoa.executeQuery();

            while (resultSetPess.next()) {

                BasicDBObject documentPessoa = new BasicDBObject();
                List<BasicDBObject> documentCadastro = new ArrayList<>();
                BasicDBObject documentLancamento = new BasicDBObject();
                List<BasicDBObject> documentLancamento_suspeso = new ArrayList<>();
                List<BasicDBObject> documentLancamento_aberto = new ArrayList<>();
                List<BasicDBObject> documentLancamento_cancelado = new ArrayList<>();
                List<BasicDBObject> documentLancamento_pago = new ArrayList<>();
                List<BasicDBObject> documentLancamento_prescrito = new ArrayList<>();

                if ("java.math.BigDecimal".equals(resultSetPess.getMetaData().getColumnClassName(1))) {
                    documentPessoa.put(resultSetPess.getMetaData().getColumnLabel(1), Double.parseDouble(resultSetPess.getObject(1).toString()));
                } else {
                    documentPessoa.put(resultSetPess.getMetaData().getColumnLabel(1), resultSetPess.getObject(1));
                }

                psmtCadastro = conn.prepareStatement(queryCadastro);
                psmtCadastro.setObject(1, resultSetPess.getObject("pess_id_pessoa"));

                ResultSet resultSetCad = psmtCadastro.executeQuery();

                int totalRowsCadastro = resultSetCad.getMetaData().getColumnCount();

                while (resultSetCad.next()) {

                    for (int i = 1; i <= totalRowsCadastro; i++) {
                        try {
                            if ("java.math.BigDecimal".equals(resultSetCad.getMetaData().getColumnClassName(i))) {
                                documentCadastro.add(new BasicDBObject(resultSetCad.getMetaData().getColumnLabel(i), Double.parseDouble(resultSetCad.getObject(i).toString())));
                            } else {
                                documentCadastro.add(new BasicDBObject(resultSetCad.getMetaData().getColumnLabel(i), resultSetCad.getObject(i)));
                            }
                        } catch (NumberFormatException | SQLException e) {
                        }
                    }

                }

                documentPessoa.put("Cadastros", documentCadastro);

                psmtLancamentos = conn.prepareStatement(queryLancamentos);
                psmtLancamentos.setObject(2, resultSetPess.getObject("pess_id_pessoa"));
                psmtLancamentos.setObject(1, resultSetPess.getObject("cred_id_credor"));

                ResultSet resultSetLanc = psmtLancamentos.executeQuery();


                while (resultSetLanc.next()) {
                    try {
                        if ("S".equals(resultSetLanc.getString("LAID_ST_SUSPENSO"))) {
                            documentLancamento_suspeso.add(new BasicDBObject("LANC_ID_LANCAMENTO", Double.parseDouble(resultSetLanc.getString("LANC_ID_LANCAMENTO"))));
                        }
                    } catch (NumberFormatException | SQLException e) {
                    }

                    try {
                        if ("S".equals(resultSetLanc.getString("LAID_ST_PRESCRITO"))) {
                            documentLancamento_prescrito.add(new BasicDBObject("LANC_ID_LANCAMENTO", Double.parseDouble(resultSetLanc.getString("LANC_ID_LANCAMENTO"))));
                        }
                    } catch (NumberFormatException | SQLException e) {
                    }

                    try {
                        if ("S".equals(resultSetLanc.getString("LAID_ST_ABERTO"))) {
                            documentLancamento_aberto.add(new BasicDBObject("LANC_ID_LANCAMENTO", Double.parseDouble(resultSetLanc.getString("LANC_ID_LANCAMENTO"))));
                        }
                    } catch (NumberFormatException | SQLException e) {
                    }

                    try {
                        if ("S".equals(resultSetLanc.getString("LAID_ST_CANCELADO"))) {
                            documentLancamento_cancelado.add(new BasicDBObject("LANC_ID_LANCAMENTO", Double.parseDouble(resultSetLanc.getString("LANC_ID_LANCAMENTO"))));
                        }
                    } catch (NumberFormatException | SQLException e) {
                    }

                    try {
                        if ("S".equals(resultSetLanc.getString("LAID_ST_PAGO"))) {
                            documentLancamento_pago.add(new BasicDBObject("LANC_ID_LANCAMENTO", Double.parseDouble(resultSetLanc.getString("LANC_ID_LANCAMENTO"))));
                        }
                    } catch (NumberFormatException | SQLException e) {
                    }

                }

                documentLancamento.put("LancamentosAberto", documentLancamento_aberto);
                documentLancamento.put("LancamentosCancelado", documentLancamento_cancelado);
                documentLancamento.put("LancamentosPago", documentLancamento_pago);
                documentLancamento.put("LancamentosSuspenso", documentLancamento_suspeso);
                documentLancamento.put("LancamentosPrescrito", documentLancamento_prescrito);

                documentPessoa.put("Lancamentos", documentLancamento);

                collection.insert(documentPessoa);

            }

            conn.close();
        } catch (NumberFormatException | SQLException e) {
        }

    }
}
