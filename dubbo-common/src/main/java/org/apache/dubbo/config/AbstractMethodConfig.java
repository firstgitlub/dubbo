/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.config;

import org.apache.dubbo.config.support.Parameter;

import java.util.Map;

/**
 * AbstractMethodConfig
 *
 * @export
 */
public abstract class AbstractMethodConfig extends AbstractConfig {

    private static final long serialVersionUID = 1L;

    /**
     * The timeout for remote invocation in milliseconds
     *
     * 远程调用的超时时间，单位为毫秒
     */
    protected Integer timeout;

    /**
     * The retry times
     * 重试次数
     */
    protected Integer retries;

    /**
     * max concurrent invocations
     *
     * 最大并发调用
     */
    protected Integer actives;

    /**
     * The load balance
     * 负载均衡
     */
    protected String loadbalance;

    /**
     * Whether to async
     * note that: it is an unreliable asynchronism that ignores return values and does not block threads.
     *
     * 是否异步请注意:忽略返回值且不阻塞线程的异步是不可靠的。
     *
     */
    protected Boolean async;

    /**
     * Whether to ack async-sent
     *
     * 是否 ack 异步发送
     */
    protected Boolean sent;

    /**
     * The name of mock class which gets called when a service fails to execute
     * <p>
     * note that: the mock doesn't support on the provider side，and the mock is executed when a non-business exception
     * occurs after a remote service call
     *
     * 当服务执行失败时调用的模拟类的名称
     *
     */
    protected String mock;

    /**
     * Merger
     *
     * 合并 并购 吸收
     *
     */
    protected String merger;

    /**
     * Cache the return result with the call parameter as key, the following options are available: lru, threadlocal,
     * jcache, etc.
     *
     * 用调用参数作为键缓存返回结果，有以下选项:lru, threadlocal, jcache等。
     *
     */
    protected String cache;

    /**
     * Whether JSR303 standard annotation validation is enabled or not, if enabled, annotations on method parameters will
     * be validated
     *
     * 无论是否启用JSR303标准注释验证，如果启用，则对方法参数上的注释进行验证
     *
     */
    protected String validation;

    /**
     * The customized parameters
     *
     * 自定义参数
     *
     */
    protected Map<String, String> parameters;

    /**
     * Forks for forking cluster
     *
     * 分叉群集
     *
     */
    protected Integer forks;

    public Integer getForks() {
        return forks;
    }

    public void setForks(Integer forks) {
        this.forks = forks;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getRetries() {
        return retries;
    }

    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    public String getLoadbalance() {
        return loadbalance;
    }

    public void setLoadbalance(String loadbalance) {
        this.loadbalance = loadbalance;
    }

    public Boolean isAsync() {
        return async;
    }

    public void setAsync(Boolean async) {
        this.async = async;
    }

    public Integer getActives() {
        return actives;
    }

    public void setActives(Integer actives) {
        this.actives = actives;
    }

    public Boolean getSent() {
        return sent;
    }

    public void setSent(Boolean sent) {
        this.sent = sent;
    }

    @Parameter(escaped = true)
    public String getMock() {
        return mock;
    }

    public void setMock(String mock) {
        this.mock = mock;
    }

    /**
     * Set the property "mock"
     *
     * @param mock the value of mock
     * @since 2.7.6
     */
    public void setMock(Object mock) {
        if (mock == null) {
            return;
        }
        this.setMock(String.valueOf(mock));
    }

    public String getMerger() {
        return merger;
    }

    public void setMerger(String merger) {
        this.merger = merger;
    }

    public String getCache() {
        return cache;
    }

    public void setCache(String cache) {
        this.cache = cache;
    }

    public String getValidation() {
        return validation;
    }

    public void setValidation(String validation) {
        this.validation = validation;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

}
