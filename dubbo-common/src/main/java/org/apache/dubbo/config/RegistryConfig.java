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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.support.Parameter;

import java.util.Map;

import static org.apache.dubbo.common.constants.CommonConstants.EXTRA_KEYS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SHUTDOWN_WAIT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PREFERRED_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_KEY;
import static org.apache.dubbo.common.constants.RemotingConstants.BACKUP_KEY;
import static org.apache.dubbo.common.utils.PojoUtils.updatePropertyIfAbsent;
import static org.apache.dubbo.config.Constants.REGISTRIES_PREFIX;

/**
 * RegistryConfig
 *
 * @export
 */
public class RegistryConfig extends AbstractConfig {

    public static final String NO_AVAILABLE = "N/A";
    public static final String PREFER_REGISTRY_KEY = REGISTRY_KEY + "." + PREFERRED_KEY;
    private static final long serialVersionUID = 5508512956753757169L;

    /**
     * Register center address
     *
     * 注册中心地址
     */
    private String address;

    /**
     * Username to login register center
     *
     * 登录注册中心的用户名
     */
    private String username;

    /**
     * Password to login register center
     *
     * 登录注册中心的密码
     */
    private String password;

    /**
     * Default port for register center
     *
     * 注册中心的端口
     */
    private Integer port;

    /**
     * Protocol for register center
     *
     * 注册中心的协议
     */
    private String protocol;

    /**
     * Network transmission type
     *
     * 网络传输类型
     */
    private String transporter;

    private String server;

    private String client;

    /**
     * Affects how traffic distributes among registries, useful when subscribing multiple registries, available options:
     * 1. zone-aware, a certain type of traffic always goes to one Registry according to where the traffic is originated.
     *
     * 影响流量在注册中心之间的分布，当订阅多个注册中心时很有用，可用选项:1。区域感知的，某种类型的流量总是根据流量的来源流向一个注册中心。
     */
    private String cluster;

    /**
     * The region where the registry belongs, usually used to isolate traffics
     *
     * 注册表所属的区域，通常用于隔离流量
     */
    private String zone;

    /**
     * The group the services registry in
     *
     * 将服务注册表分组
     */
    private String group;

    private String version;

    /**
     * Request timeout in milliseconds for register center
     *
     * 请求注册中心超时，以毫秒为单位
     */
    private Integer timeout;

    /**
     * Session timeout in milliseconds for register center
     *
     * 注册中心的会话超时，以毫秒为单位
     *
     */
    private Integer session;

    /**
     * File for saving register center dynamic list
     *
     * 用于保存注册中心动态列表的文件
     */
    private String file;

    /**
     * Wait time before stop
     *
     * 停前等待时间
     */
    private Integer wait;

    /**
     * Whether to check if register center is available when boot up
     *
     * 开机时是否检查注册中心是否可用
     */
    private Boolean check;

    /**
     * Whether to allow dynamic service to register on the register center
     *
     * 是否允许动态服务在注册中心注册
     */
    private Boolean dynamic;

    /**
     * Whether to export service on the register center
     *
     * 是否在注册中心导出服务
     */
    private Boolean register;

    /**
     * Whether allow to subscribe service on the register center
     *
     * 是否允许在注册中心订阅服务
     */
    private Boolean subscribe;

    /**
     * The customized parameters
     *
     * 自定义参数
     */
    private Map<String, String> parameters;

    /**
     * Simple the registry. both useful for provider and consumer
     *
     * 简单的注册表。对提供者和使用者都有用
     *
     * @since 2.7.0
     */
    private Boolean simplified;
    /**
     * After simplify the registry, should add some parameter individually. just for provider.
     * <p>
     * such as: extra-keys = A,b,c,d
     *
     * 简化注册后，应分别添加一些参数。只是为了提供者。
     *
     * @since 2.7.0
     */
    private String extraKeys;

    /**
     * the address work as config center or not
     *
     * 该地址是否作为配置中心工作
     */
    private Boolean useAsConfigCenter;

    /**
     * the address work as remote metadata center or not
     *
     * 该地址是否作为远程元数据中心
     */
    private Boolean useAsMetadataCenter;

    /**
     * list of rpc protocols accepted by this registry, for example, "dubbo,rest"
     *
     * 此注册中心接受的RPC协议列表，例如，“dubbo,rest”
     */
    private String accepts;

    /**
     * Always use this registry first if set to true, useful when subscribe to multiple registries
     *
     * 如果设置为true，请始终先使用此注册表，在订阅多个注册表时很有用
     */
    private Boolean preferred;

    /**
     * Affects traffic distribution among registries, useful when subscribe to multiple registries
     * Take effect only when no preferred registry is specified.
     *
     * 影响注册表之间的流量分配，在订阅多个注册表时有用。仅在没有指定首选注册表时生效。
     *
     */
    private Integer weight;

    public RegistryConfig() {
    }

    public RegistryConfig(String address) {
        setAddress(address);
    }

    public RegistryConfig(String address, String protocol) {
        setAddress(address);
        setProtocol(protocol);
    }

    @Override
    public String getId() {
        return super.getId();
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
//        this.updateIdIfAbsent(protocol);
    }

    @Parameter(excluded = true)
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
        if (address != null) {
            try {
                URL url = URL.valueOf(address);

                // Refactor since 2.7.8
                updatePropertyIfAbsent(this::getUsername, this::setUsername, url.getUsername());
                updatePropertyIfAbsent(this::getPassword, this::setPassword, url.getPassword());
                updatePropertyIfAbsent(this::getProtocol, this::setProtocol, url.getProtocol());
                updatePropertyIfAbsent(this::getPort, this::setPort, url.getPort());

                Map<String, String> params = url.getParameters();
                if (CollectionUtils.isNotEmptyMap(params)) {
                    params.remove(BACKUP_KEY);
                }
                updateParameters(params);
            } catch (Exception ignored) {
            }
        }
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return wait
     * @see org.apache.dubbo.config.ProviderConfig#getWait()
     * @deprecated
     */
    @Deprecated
    public Integer getWait() {
        return wait;
    }

    /**
     * @param wait
     * @see org.apache.dubbo.config.ProviderConfig#setWait(Integer)
     * @deprecated
     */
    @Deprecated
    public void setWait(Integer wait) {
        this.wait = wait;
        if (wait != null && wait > 0) {
            System.setProperty(SHUTDOWN_WAIT_KEY, String.valueOf(wait));
        }
    }

    public Boolean isCheck() {
        return check;
    }

    public void setCheck(Boolean check) {
        this.check = check;
    }

    public String getFile() {
        return file;
    }

    public void setFile(String file) {
        this.file = file;
    }

    /**
     * @return transport
     * @see #getTransporter()
     * @deprecated
     */
    @Deprecated
    @Parameter(excluded = true)
    public String getTransport() {
        return getTransporter();
    }

    /**
     * @param transport
     * @see #setTransporter(String)
     * @deprecated
     */
    @Deprecated
    public void setTransport(String transport) {
        setTransporter(transport);
    }

    public String getTransporter() {
        return transporter;
    }

    public void setTransporter(String transporter) {
        /*if(transporter != null && transporter.length() > 0 && ! ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(transporter)){
            throw new IllegalStateException("No such transporter type : " + transporter);
        }*/
        this.transporter = transporter;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        /*if(server != null && server.length() > 0 && ! ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(server)){
            throw new IllegalStateException("No such server type : " + server);
        }*/
        this.server = server;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        /*if(client != null && client.length() > 0 && ! ExtensionLoader.getExtensionLoader(Transporter.class).hasExtension(client)){
            throw new IllegalStateException("No such client type : " + client);
        }*/
        this.client = client;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getSession() {
        return session;
    }

    public void setSession(Integer session) {
        this.session = session;
    }

    public Boolean isDynamic() {
        return dynamic;
    }

    public void setDynamic(Boolean dynamic) {
        this.dynamic = dynamic;
    }

    public Boolean isRegister() {
        return register;
    }

    public void setRegister(Boolean register) {
        this.register = register;
    }

    public Boolean isSubscribe() {
        return subscribe;
    }

    public void setSubscribe(Boolean subscribe) {
        this.subscribe = subscribe;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public void updateParameters(Map<String, String> parameters) {
        if (CollectionUtils.isEmptyMap(parameters)) {
            return;
        }
        if (this.parameters == null) {
            this.parameters = parameters;
        } else {
            this.parameters.putAll(parameters);
        }
    }

    public Boolean getSimplified() {
        return simplified;
    }

    public void setSimplified(Boolean simplified) {
        this.simplified = simplified;
    }

    @Parameter(key = EXTRA_KEYS_KEY)
    public String getExtraKeys() {
        return extraKeys;
    }

    public void setExtraKeys(String extraKeys) {
        this.extraKeys = extraKeys;
    }

    @Parameter(excluded = true)
    public Boolean getUseAsConfigCenter() {
        return useAsConfigCenter;
    }

    public void setUseAsConfigCenter(Boolean useAsConfigCenter) {
        this.useAsConfigCenter = useAsConfigCenter;
    }

    @Parameter(excluded = true)
    public Boolean getUseAsMetadataCenter() {
        return useAsMetadataCenter;
    }

    public void setUseAsMetadataCenter(Boolean useAsMetadataCenter) {
        this.useAsMetadataCenter = useAsMetadataCenter;
    }

    public String getAccepts() {
        return accepts;
    }

    public void setAccepts(String accepts) {
        this.accepts = accepts;
    }

    @Parameter(key = PREFER_REGISTRY_KEY)
    public Boolean getPreferred() {
        return preferred;
    }

    public void setPreferred(Boolean preferred) {
        this.preferred = preferred;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    @Override
    public void refresh() {
        super.refresh();
        if (StringUtils.isNotEmpty(this.getId())) {
            this.setPrefix(REGISTRIES_PREFIX);
            super.refresh();
        }
    }

    @Override
    @Parameter(excluded = true)
    public boolean isValid() {
        // empty protocol will default to 'dubbo'
        return !StringUtils.isEmpty(address);
    }
}
