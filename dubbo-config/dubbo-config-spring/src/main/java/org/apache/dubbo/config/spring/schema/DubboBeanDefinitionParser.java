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
package org.apache.dubbo.config.spring.schema;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.AbstractServiceConfig;
import org.apache.dubbo.config.ArgumentConfig;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.MethodConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.spring.ReferenceBean;
import org.apache.dubbo.config.spring.ServiceBean;

import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanDefinitionHolder;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.ManagedList;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.env.Environment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.dubbo.common.constants.CommonConstants.HIDDEN_KEY_PREFIX;

/**
 * AbstractBeanDefinitionParser
 *
 * @export
 */
public class DubboBeanDefinitionParser implements BeanDefinitionParser {

    private static final Logger logger = LoggerFactory.getLogger(DubboBeanDefinitionParser.class);
    private static final Pattern GROUP_AND_VERSION = Pattern.compile("^[\\-.0-9_a-zA-Z]+(\\:[\\-.0-9_a-zA-Z]+)?$");
    private static final String ONRETURN = "onreturn";
    private static final String ONTHROW = "onthrow";
    private static final String ONINVOKE = "oninvoke";
    private static final String METHOD = "Method";
    private final Class<?> beanClass;
    private final boolean required;

    public DubboBeanDefinitionParser(Class<?> beanClass, boolean required) {
        this.beanClass = beanClass;
        this.required = required;
    }

    @SuppressWarnings("unchecked")
    private static RootBeanDefinition parse(Element element, ParserContext parserContext, Class<?> beanClass, boolean required) {
        RootBeanDefinition beanDefinition = new RootBeanDefinition();
        // 根据构造方法传入的类型设置beanClass
        beanDefinition.setBeanClass(beanClass);
        // 设置懒加载为false
        beanDefinition.setLazyInit(false);
        String id = resolveAttribute(element, "id", parserContext);
        // 如果id属性为空，并且构造方法传入的required为true
        if (StringUtils.isEmpty(id) && required) {
            // 生成的beanName默认为name属性值
            String generatedBeanName = resolveAttribute(element, "name", parserContext);
            // 如果name属性为空
            if (StringUtils.isEmpty(generatedBeanName)) {
                if (ProtocolConfig.class.equals(beanClass)) {
                    // 如果解析的是<dubbo:protocol/>标签，设置beanName为dubbo
                    generatedBeanName = "dubbo";
                } else {
                    // 否则beanName赋值为interface属性值
                    generatedBeanName = resolveAttribute(element, "interface", parserContext);
                }
            }
            if (StringUtils.isEmpty(generatedBeanName)) {
                // 如果beanName还是为空，则将其设置为beanClass的名称
                generatedBeanName = beanClass.getName();
            }
            // 将beanName赋值给id
            id = generatedBeanName;
            int counter = 2;
            // 循环判断如果当前Spring上下文中包含当前id，则将id拼接递增数字后缀
            while (parserContext.getRegistry().containsBeanDefinition(id)) {
                id = generatedBeanName + (counter++);
            }
        }
        if (StringUtils.isNotEmpty(id)) {
            // 如果到这里 判断 如果 当前Spring上下文 中包含当前bean id，则抛出bean id冲突的异常
            if (parserContext.getRegistry().containsBeanDefinition(id)) {
                throw new IllegalStateException("Duplicate spring bean id " + id);
            }
            // 注册BeanDefinition
            parserContext.getRegistry().registerBeanDefinition(id, beanDefinition);
            // 添加id属性值
            beanDefinition.getPropertyValues().addPropertyValue("id", id);
        }
        // <dubbo:protocol/>标签
        if (ProtocolConfig.class.equals(beanClass)) {
            for (String name : parserContext.getRegistry().getBeanDefinitionNames()) {
                BeanDefinition definition = parserContext.getRegistry().getBeanDefinition(name);
                PropertyValue property = definition.getPropertyValues().getPropertyValue("protocol");
                // 遍历所有的BeanDefinition，判断是否有protocol属性
                if (property != null) {
                    Object value = property.getValue();
                    // dubbo 协议的话 会有对应的 建立协议的链接信息等
                    if (value instanceof ProtocolConfig && id.equals(((ProtocolConfig) value).getName())) {
                        // 如果有并且是ProtocolConfig类型则为其添加对当前bean id的依赖
                        definition.getPropertyValues().addPropertyValue("protocol", new RuntimeBeanReference(id));
                    }
                }
            }
        }
        // <dubbo:service/>标签
        else if (ServiceBean.class.equals(beanClass)) {
            // 获取class属性
            String className = resolveAttribute(element, "class", parserContext);
            if (StringUtils.isNotEmpty(className)) {
                // 构建配置的class的BeanDefinition
                RootBeanDefinition classDefinition = new RootBeanDefinition();
                // 设置beanClass
                classDefinition.setBeanClass(ReflectUtils.forName(className));
                classDefinition.setLazyInit(false);
                /* 解析<property/>子标签 */
                parseProperties(element.getChildNodes(), classDefinition, parserContext);
                // 添加ServiceBean ref属性的依赖
                beanDefinition.getPropertyValues().addPropertyValue("ref", new BeanDefinitionHolder(classDefinition, id + "Impl"));
            }
        }
        // <dubbo:provider/>标签
        else if (ProviderConfig.class.equals(beanClass)) {
            /* 解析嵌套的元素 */
            parseNested(element, parserContext, ServiceBean.class, true, "service", "provider", id, beanDefinition);
        }
        // <dubbo:consumer/>标签
        else if (ConsumerConfig.class.equals(beanClass)) {
            /* 解析嵌套的元素 */
            parseNested(element, parserContext, ReferenceBean.class, false, "reference", "consumer", id, beanDefinition);
        }
        Set<String> props = new HashSet<>();
        ManagedMap parameters = null;

        // 遍历 beanClass 的方法
        for (Method setter : beanClass.getMethods()) {
            String name = setter.getName();
            // 判断是否是public的有参数的setter方法
            if (name.length() > 3 && name.startsWith("set")
                    && Modifier.isPublic(setter.getModifiers())
                    && setter.getParameterTypes().length == 1) {
                Class<?> type = setter.getParameterTypes()[0];
                String beanProperty = name.substring(3, 4).toLowerCase() + name.substring(4);

                // 将setter驼峰命名去掉set后转成-连接的命名，如setApplicationContext --> application-context
                String property = StringUtils.camelToSplitName(beanProperty, "-");
                props.add(property);
                // check the setter/getter whether match
                Method getter = null;
                try {
                    // 获取对应属性的getter方法
                    getter = beanClass.getMethod("get" + name.substring(3), new Class<?>[0]);
                } catch (NoSuchMethodException e) {
                    try {
                        // boolean 类型的属性的 getter 方法可能以is开头
                        getter = beanClass.getMethod("is" + name.substring(3), new Class<?>[0]);
                    } catch (NoSuchMethodException e2) {
                        // ignore, there is no need any log here since some class implement the interface: EnvironmentAware,
                        // ApplicationAware, etc. They only have setter method, otherwise will cause the error log during application start up.
                    }
                }
                // 如果没有getter方法或者getter方法不是public修饰符或者setter方法的参数类型与getter方法的返回值类型不同，直接忽略
                if (getter == null
                        || !Modifier.isPublic(getter.getModifiers())
                        || !type.equals(getter.getReturnType())) {
                    continue;
                }
                if ("parameters".equals(property)) {
                    /* parameters属性解析 */
                    parameters = parseParameters(element.getChildNodes(), beanDefinition, parserContext);
                } else if ("methods".equals(property)) {
                    /* methods属性解析 */
                    parseMethods(id, element.getChildNodes(), beanDefinition, parserContext);
                } else if ("arguments".equals(property)) {
                    /* arguments属性解析 */
                    parseArguments(id, element.getChildNodes(), beanDefinition, parserContext);
                } else {

                    // 获取元素中的对应属性值
                    String value = resolveAttribute(element, property, parserContext);
                    if (value != null) {
                        value = value.trim();
                        if (value.length() > 0) {
                            // registry属性设置为N/A
                            if ("registry".equals(property) && RegistryConfig.NO_AVAILABLE.equalsIgnoreCase(value)) {
                                RegistryConfig registryConfig = new RegistryConfig();
                                registryConfig.setAddress(RegistryConfig.NO_AVAILABLE);
                                beanDefinition.getPropertyValues().addPropertyValue(beanProperty, registryConfig);
                            } else if ("provider".equals(property) || "registry".equals(property) || ("protocol".equals(property) && AbstractServiceConfig.class.isAssignableFrom(beanClass))) {
                                /**
                                 * For 'provider' 'protocol' 'registry', keep literal value (should be id/name) and set the value to 'registryIds' 'providerIds' protocolIds'
                                 * The following process should make sure each id refers to the corresponding instance, here's how to find the instance for different use cases:
                                 * 1. Spring, check existing bean by id, see{@link ServiceBean#afterPropertiesSet()}; then try to use id to find configs defined in remote Config Center
                                 * 2. API, directly use id to find configs defined in remote Config Center; if all config instances are defined locally, please use {@link ServiceConfig#setRegistries(List)}
                                 */
                                /* 多值registry设置 */
                                /* 多值provider设置 */
                                /* 多值protocol设置 */
                                beanDefinition.getPropertyValues().addPropertyValue(beanProperty + "Ids", value);
                            } else {
                                Object reference;
                                // 判断方法的参数是否是基本类型，包括包装类型
                                if (isPrimitive(type)) {
                                    if ("async".equals(property) && "false".equals(value)
                                            || "timeout".equals(property) && "0".equals(value)
                                            || "delay".equals(property) && "0".equals(value)
                                            || "version".equals(property) && "0.0.0".equals(value)
                                            || "stat".equals(property) && "-1".equals(value)
                                            || "reliable".equals(property) && "false".equals(value)) {
                                        // backward compatibility for the default value in old version's xsd
                                        // 向后兼容旧版本的xsd中的默认值
                                        value = null;
                                    }
                                    reference = value;
                                } else if (ONRETURN.equals(property) || ONTHROW.equals(property) || ONINVOKE.equals(property)) {
                                    int index = value.lastIndexOf(".");
                                    String ref = value.substring(0, index);
                                    String method = value.substring(index + 1);
                                    reference = new RuntimeBeanReference(ref);
                                    beanDefinition.getPropertyValues().addPropertyValue(property + METHOD, method);
                                } else {
                                    if ("ref".equals(property) && parserContext.getRegistry().containsBeanDefinition(value)) {
                                        BeanDefinition refBean = parserContext.getRegistry().getBeanDefinition(value);
                                        if (!refBean.isSingleton()) {
                                            throw new IllegalStateException("The exported service ref " + value + " must be singleton! Please set the " + value + " bean scope to singleton, eg: <bean id=\"" + value + "\" scope=\"singleton\" ...>");
                                        }
                                    }
                                    reference = new RuntimeBeanReference(value);
                                }
                                beanDefinition.getPropertyValues().addPropertyValue(beanProperty, reference);
                            }
                        }
                    }
                }
            }
        }

        // 排除掉上面解析过的，剩余的属性添加到parameters属性中
        NamedNodeMap attributes = element.getAttributes();
        int len = attributes.getLength();
        for (int i = 0; i < len; i++) {
            Node node = attributes.item(i);
            String name = node.getLocalName();
            if (!props.contains(name)) {
                if (parameters == null) {
                    parameters = new ManagedMap();
                }
                String value = node.getNodeValue();
                parameters.put(name, new TypedStringValue(value, String.class));
            }
        }
        if (parameters != null) {
            beanDefinition.getPropertyValues().addPropertyValue("parameters", parameters);
        }
        return beanDefinition;
    }

    private static boolean isPrimitive(Class<?> cls) {
        return cls.isPrimitive() || cls == Boolean.class || cls == Byte.class
                || cls == Character.class || cls == Short.class || cls == Integer.class
                || cls == Long.class || cls == Float.class || cls == Double.class
                || cls == String.class || cls == Date.class || cls == Class.class;
    }

    private static void parseNested(Element element, ParserContext parserContext, Class<?> beanClass, boolean required, String tag, String property, String ref, BeanDefinition beanDefinition) {
        NodeList nodeList = element.getChildNodes();
        if (nodeList == null) {
            return;
        }
        boolean first = true;

        // 如果子节点不为null，遍历子节点
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            if (!(node instanceof Element)) {
                continue;
            }

            // 判断节点名称是否与标签名称相同
            if (tag.equals(node.getNodeName())
                    || tag.equals(node.getLocalName())) {
                if (first) {
                    first = false;
                    String isDefault = resolveAttribute(element, "default", parserContext);
                    if (StringUtils.isEmpty(isDefault)) {
                        // 如果第一个子节点default属性为null，则设置为false
                        beanDefinition.getPropertyValues().addPropertyValue("default", "false");
                    }
                }
                // 递归解析嵌套的子节点
                BeanDefinition subDefinition = parse((Element) node, parserContext, beanClass, required);
                if (subDefinition != null && StringUtils.isNotEmpty(ref)) {
                    // 添加属性依赖
                    subDefinition.getPropertyValues().addPropertyValue(property, new RuntimeBeanReference(ref));
                }
            }
        }
    }

    private static void parseProperties(NodeList nodeList, RootBeanDefinition beanDefinition, ParserContext parserContext) {
        if (nodeList == null) {
            return;
        }


        // 如果子节点不为null，遍历子节点
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);
            // <property/>子标签
            if ("property".equals(element.getNodeName())
                    || "property".equals(element.getLocalName())) {
                String name = resolveAttribute(element, "name", parserContext);
                if (StringUtils.isNotEmpty(name)) {
                    // 提取value属性
                    String value = resolveAttribute(element, "value", parserContext);
                    // 提取ref属性
                    String ref = resolveAttribute(element, "ref", parserContext);
                    if (StringUtils.isNotEmpty(value)) {
                        // value不为null，添加对应属性值
                        beanDefinition.getPropertyValues().addPropertyValue(name, value);
                    } else if (StringUtils.isNotEmpty(ref)) {
                        // ref不为null，添加对应属性依赖
                        beanDefinition.getPropertyValues().addPropertyValue(name, new RuntimeBeanReference(ref));
                    } else {
                        throw new UnsupportedOperationException("Unsupported <property name=\"" + name + "\"> sub tag, Only supported <property name=\"" + name + "\" ref=\"...\" /> or <property name=\"" + name + "\" value=\"...\" />");
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static ManagedMap parseParameters(NodeList nodeList, RootBeanDefinition beanDefinition, ParserContext parserContext) {
        if (nodeList == null) {
            return null;
        }
        ManagedMap parameters = null;

        // 如果子节点不为null，遍历子节点
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);

            // 判断子节点名称是否是parameter
            if ("parameter".equals(element.getNodeName())
                    || "parameter".equals(element.getLocalName())) {
                if (parameters == null) {
                    parameters = new ManagedMap();
                }
                // 提取key属性值
                String key = resolveAttribute(element, "key", parserContext);
                // 提取value属性值
                String value = resolveAttribute(element, "value", parserContext);
                // 判断是否设置hide为true
                boolean hide = "true".equals(resolveAttribute(element, "hide", parserContext));
                if (hide) {

                    // 如果设置了hide为true，则为key增加.前缀
                    key = HIDDEN_KEY_PREFIX + key;
                }
                parameters.put(key, new TypedStringValue(value, String.class));
            }
        }
        return parameters;
    }

    @SuppressWarnings("unchecked")
    private static void parseMethods(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                     ParserContext parserContext) {
        if (nodeList == null) {
            return;
        }
        ManagedList methods = null;

        // 如果子节点不为null，遍历子节点
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);
            // 判断子节点名称是否是method
            if ("method".equals(element.getNodeName()) || "method".equals(element.getLocalName())) {

                // 提取name属性值
                String methodName = resolveAttribute(element, "name", parserContext);
                // name属性为null抛出异常
                if (StringUtils.isEmpty(methodName)) {
                    throw new IllegalStateException("<dubbo:method> name attribute == null");
                }
                if (methods == null) {
                    methods = new ManagedList();
                }
                // 递归解析method子节点
                RootBeanDefinition methodBeanDefinition = parse(element,
                        parserContext, MethodConfig.class, false);

                // 拼接name
                String beanName = id + "." + methodName;

                // If the PropertyValue named "id" can't be found,
                // bean name will be taken as the "id" PropertyValue for MethodConfig
                if (!hasPropertyValue(methodBeanDefinition, "id")) {
                    addPropertyValue(methodBeanDefinition, "id", beanName);
                }

                // 构造BeanDefinitionHolder
                BeanDefinitionHolder methodBeanDefinitionHolder = new BeanDefinitionHolder(
                        methodBeanDefinition, beanName);
                methods.add(methodBeanDefinitionHolder);
            }
        }
        if (methods != null) {
            // 如果不为null，添加对应属性的依赖
            beanDefinition.getPropertyValues().addPropertyValue("methods", methods);
        }
    }

    private static boolean hasPropertyValue(AbstractBeanDefinition beanDefinition, String propertyName) {
        return beanDefinition.getPropertyValues().contains(propertyName);
    }

    private static void addPropertyValue(AbstractBeanDefinition beanDefinition, String propertyName, String propertyValue) {
        if (StringUtils.isBlank(propertyName) || StringUtils.isBlank(propertyValue)) {
            return;
        }
        beanDefinition.getPropertyValues().addPropertyValue(propertyName, propertyValue);
    }

    @SuppressWarnings("unchecked")
    private static void parseArguments(String id, NodeList nodeList, RootBeanDefinition beanDefinition,
                                       ParserContext parserContext) {
        if (nodeList == null) {
            return;
        }
        ManagedList arguments = null;

        // 如果子节点不为null，遍历子节点
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element)) {
                continue;
            }
            Element element = (Element) nodeList.item(i);

            // 判断子节点名称是否是argument
            if ("argument".equals(element.getNodeName()) || "argument".equals(element.getLocalName())) {

                // 提取index属性值
                String argumentIndex = resolveAttribute(element, "index", parserContext);
                if (arguments == null) {
                    arguments = new ManagedList();
                }

                // 递归解析argument子节点
                BeanDefinition argumentBeanDefinition = parse(element,
                        parserContext, ArgumentConfig.class, false);

                // 拼接name
                String name = id + "." + argumentIndex;

                // 构造BeanDefinitionHolder
                BeanDefinitionHolder argumentBeanDefinitionHolder = new BeanDefinitionHolder(
                        argumentBeanDefinition, name);
                arguments.add(argumentBeanDefinitionHolder);
            }
        }
        if (arguments != null) {

            // 如果不为null，添加对应属性的依赖
            beanDefinition.getPropertyValues().addPropertyValue("arguments", arguments);
        }
    }

    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {
        /* 解析 */
        return parse(element, parserContext, beanClass, required);
    }

    private static String resolveAttribute(Element element, String attributeName, ParserContext parserContext) {
        String attributeValue = element.getAttribute(attributeName);
        Environment environment = parserContext.getReaderContext().getEnvironment();
        return environment.resolvePlaceholders(attributeValue);
    }
}
