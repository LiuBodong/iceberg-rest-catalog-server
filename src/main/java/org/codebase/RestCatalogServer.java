/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codebase;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.codebase.config.ServerConfig;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.InetSocketAddress;

public class RestCatalogServer {

    private final Catalog catalog;
    private final ServerConfig serverConfig;

    public RestCatalogServer(Catalog catalog, ServerConfig serverConfig) {
        this.catalog = catalog;
        this.serverConfig = serverConfig;
    }

    public void start() throws Exception {
        try (RESTCatalogAdapter restCatalogAdapter = new RESTCatalogAdapter(catalog)) {
            ServletContextHandler servletContext = getServletContextHandler(restCatalogAdapter);
            InetSocketAddress inetSocketAddress = new InetSocketAddress(serverConfig.bindHost(), serverConfig.bindPort());
            Server httpServer = new Server(inetSocketAddress);
            httpServer.setHandler(servletContext);
            httpServer.start();
            httpServer.join();
        }
    }

    private static ServletContextHandler getServletContextHandler(RESTCatalogAdapter restCatalogAdapter) {
        RESTCatalogServlet restCatalogServlet = new RESTCatalogServlet(restCatalogAdapter);
        ServletContextHandler servletContext = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        servletContext.setContextPath("/");
        ServletHolder servletHolder = new ServletHolder(restCatalogServlet);
        servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
        servletContext.addServlet(servletHolder, "/*");
        servletContext.setVirtualHosts(null);
        servletContext.setGzipHandler(new GzipHandler());
        return servletContext;
    }
}
