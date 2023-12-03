package oracle.datahub.spark.server;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Test extends HttpServlet {
  private static final long serialVersionUID = 1L;
  @Override
  protected void doGet(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println("{ \"status\": \"ok\"}");
  }
}
