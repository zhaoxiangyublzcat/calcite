/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Base class for CREATE, DROP and other DDL statements. */
public abstract class SqlDdl extends SqlCall implements DdlExecutor, ReflectiveVisitor {
  /** Use this operator only if you don't have a better one. */
  protected static final SqlOperator DDL_OPERATOR =
      new SqlSpecialOperator("DDL", SqlKind.OTHER_DDL);

  private final SqlOperator operator;

  /** Creates a SqlDdl. */
  protected SqlDdl(SqlOperator operator, SqlParserPos pos) {
    super(pos);
    this.operator = requireNonNull(operator, "operator");
  }

  @Override public SqlOperator getOperator() {
    return operator;
  }

  @SuppressWarnings({"method.invocation.invalid", "argument.type.incompatible"})
  private final ReflectUtil.MethodDispatcher<Void> dispatcher =
      ReflectUtil.createMethodDispatcher(void.class, this, "execute",
          SqlNode.class, CalcitePrepare.Context.class);

  @Override public void executeDdl(CalcitePrepare.Context context,
      SqlNode node) {
    dispatcher.invoke(node, context);
  }

  public void execute(SqlNode node, CalcitePrepare.Context context) {
    throw new UnsupportedOperationException("DDL not supported: " + node);
  }

  protected static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context,
      boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema =
        mutable ? context.getMutableRootSchema()
            : context.getRootSchema();
    for (String p : path) {
      schema = requireNonNull(schema.getSubSchema(p, true));
    }
    return Pair.of(schema, name);
  }

}
