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
package org.apache.asterix.om.functions;

import java.util.List;
import java.util.Map;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression.FunctionKind;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class ExternalFunctionInfo extends FunctionInfo implements IExternalFunctionInfo {

    private static final long serialVersionUID = 1L;

    private final transient IResultTypeComputer rtc;
    private final List<IAType> argumentTypes;
    private final String body;
    private final String language;
    private final FunctionKind kind;
    private final IAType returnType;
    private final String library;
    private final Map<String, String> params;

    public ExternalFunctionInfo(String namespace, String name, int arity, FunctionKind kind, List<IAType> argumentTypes,
            IAType returnType, IResultTypeComputer rtc, String body, String language, String library,
            Map<String, String> params) {
        this(new FunctionIdentifier(namespace, name, arity), kind, argumentTypes, returnType, rtc, body, library,
                language, params);
    }

    public ExternalFunctionInfo(FunctionIdentifier fid, FunctionKind kind, List<IAType> argumentTypes,
            IAType returnType, IResultTypeComputer rtc, String body, String language, String library,
            Map<String, String> params) {
        // TODO: fix CheckNonFunctionalExpressionVisitor once we have non-functional external functions
        super(fid, true);
        this.rtc = rtc;
        this.argumentTypes = argumentTypes;
        this.body = body;
        this.library = library;
        this.language = language;
        this.kind = kind;
        this.returnType = returnType;
        this.params = params;
    }

    public IResultTypeComputer getResultTypeComputer() {
        return rtc;
    }

    public List<IAType> getArgumentTypes() {
        return argumentTypes;
    }

    @Override
    public String getFunctionBody() {
        return body;
    }

    @Override
    public List<IAType> getArgumentList() {
        return argumentTypes;
    }

    @Override
    public String getLanguage() {
        return language;
    }

    @Override
    public FunctionKind getKind() {
        return kind;
    }

    @Override
    public IAType getReturnType() {
        return returnType;
    }

    @Override
    public String getLibrary() {
        return library;
    }

    @Override
    public Map<String, String> getParams() {
        return params;
    }

}
