package cn.thinkingdata.tga.javasdk.test.functionTest;

import cn.thinkingdata.tga.javasdk.ThinkingDataAnalytics;
import cn.thinkingdata.tga.javasdk.exception.InvalidArgumentException;
import org.apache.http.util.TextUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

interface ISDKCase<T> {
    T doAction();
}

interface ISDKTest<T> {
    void run();

    ISDKTest addVerify(ISDKVerify userIDVerify);
}

interface ISDKVerify<T> {
    void verify(T e);
}

class SDKTest<T> implements ISDKTest {
    List<ISDKCase<T>> mCases = new ArrayList<ISDKCase<T>>();
    List<ISDKVerify> mVerifies = new ArrayList<ISDKVerify>();

    public SDKTest addCase(ISDKCase<T> _case) {
        this.mCases.add(_case);
        return this;
    }

    public SDKTest addVerify(ISDKVerify userIDVerify) {
        if (userIDVerify != null) {
            mVerifies.add(userIDVerify);
        }
        return this;
    }

    @Override
    public void run() {
        for (ISDKCase<T> _case : mCases) {
            T e = _case.doAction();
            for (ISDKVerify verify : mVerifies) {
                verify.verify(e);
            }
        }
    }

    public void reset() {
        mCases.clear();
        mVerifies.clear();
    }
}

public class AssembleEventTest {
    private ThinkingDataAnalytics sdk;
    private TemporaryConsumer consumer;
    private boolean isStrictModel = false;

    @Before
    public void init() {
        System.out.println("before");
        consumer = new TemporaryConsumer();
        sdk = new ThinkingDataAnalytics(consumer, false, isStrictModel);
    }

    @After
    public void after() {
        System.out.println("After");
    }

    void setUpEventCase(final String accountId, final String distinctId, final String eventName,
                        final Map<String, Object> properties, SDKTest sdkTest) {
        this.<Exception>setUpEventCase(accountId, distinctId, eventName, properties, sdkTest, null, null, null, null);
    }

    <T> void setUpEventCase(final String accountId, final String distinctId, final String eventName,
                            final Map<String, Object> properties, SDKTest sdkTest, final T... result) {
        sdkTest.addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    System.out.println("[track]properties=" + properties);
                    sdk.track(accountId, distinctId, eventName, properties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[0];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    System.out.println("[track_update]properties=" + properties);
                    sdk.track_update(accountId, distinctId, eventName, "eventId", properties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[1];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    System.out.println("[track_overwrite]properties=" + properties);
                    sdk.track_overwrite(accountId, distinctId, eventName, "eventId", properties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[2];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    if (!properties.containsKey("#first_check_id")) {
                        properties.put("#first_check_id", "123");
                    }
                    System.out.println("[track_first]properties=" + properties);
                    sdk.track_first(accountId, distinctId, eventName, properties);
                    properties.remove("#first_check_id");
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[3];
            }
        });
    }

    void setUpUserPropertyCase(final String accountId, final String distinctId, final Map<String, Object> properties,
                               SDKTest sdkTest) {
        this.<Exception>setUpUserPropertyCase(accountId, distinctId, properties, sdkTest, null, null, null, null, null,
                null, null);
    }

    <T> void setUpUserPropertyCase(final String accountId, final String distinctId,
                                   final Map<String, Object> properties, SDKTest sdkTest, final T... result) {
        sdkTest.addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    System.out.println("[user_set]properties=" + properties);
                    sdk.user_set(accountId, distinctId, properties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[0];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    System.out.println("[user_setOnce]properties=" + properties);
                    sdk.user_setOnce(accountId, distinctId, properties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[1];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    Map<String, Object> appendProperties = new HashMap<>();
                    appendProperties.put("list", Arrays.asList("a", "b", "c"));
                    appendProperties.putAll(properties);
                    System.out.println("[user_append]properties=" + properties);
                    sdk.user_append(accountId, distinctId, appendProperties);

                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[2];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    System.out.println("[user_unset]properties=" + properties);
                    sdk.user_unset(accountId, distinctId, "a");
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[3];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                Map<String, Object> addProperties = new HashMap<>();
                addProperties.put("age", 1);
                for (String key : properties.keySet()) {
                    Object value = properties.get(key);
                    if (value instanceof Number || key.startsWith("#")) {
                        addProperties.put(key, value);
                    }
                }

                try {
                    System.out.println("[user_add]properties=" + properties);
                    sdk.user_add(accountId, distinctId, addProperties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[4];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    System.out.println("[user_del]properties=" + properties);
                    sdk.user_del(accountId, distinctId);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[5];
            }
        }).addCase(new ISDKCase<T>() {
            @Override
            public T doAction() {
                try {
                    Map<String, Object> appendProperties = new HashMap<>();
                    appendProperties.put("list", Arrays.asList("a", "b", "c"));
                    appendProperties.putAll(properties);
                    System.out.println("[user_uniqAppend]properties=" + properties);
                    sdk.user_uniqAppend(accountId, distinctId, appendProperties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result[6];
            }
        });
    }

    <T> SDKTest setUpUserAdd(final String accountId, final String distinctId, final Map<String, Object> properties,
                                                              SDKTest sdkTest, final T result) {
        sdkTest.addCase(new ISDKCase() {
            @Override
            public Object doAction() {
                try {
                    sdk.user_add(accountId, distinctId, properties);
                } catch (InvalidArgumentException e) {
                    return (T) e;
                }
                return result;
            }
        });
        return sdkTest;
    }

    void initUserIDCase(final String accountId, final String distinctId, final String eventName, SDKTest sdkTest) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");
        setUpEventCase(accountId, distinctId, eventName, properties, sdkTest);
        setUpUserPropertyCase(accountId, distinctId, properties, sdkTest);
    }

    @Test
    public void testEmptyAccount_EmptyDistinctId() {
        ISDKVerify emptyVerify = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                if (isStrictModel) {
                    Assert.assertNotNull(e);
                    Assert.assertEquals("accountId or distinctId must be provided.", e.getMessage());
                }

            }
        };
        SDKTest test = new SDKTest();
        initUserIDCase("", "", "eventName", test);
        test.addVerify(emptyVerify);
        test.run();
        test.reset();
    }

    @Test
    public void testEmptyDistinctId() {
        ISDKVerify onlyAccountIdVerity = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                Assert.assertNull(e);
                Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
                Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getDistinctId()));
            }
        };
        SDKTest test = new SDKTest();
        initUserIDCase("accountId", "", "eventName", test);
        test.addVerify(onlyAccountIdVerity);
        test.run();
        test.reset();
    }

    @Test
    public void testEmptyAccountId() {
        ISDKVerify onlyDistinctIdVerity = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                Assert.assertNull(e);
                Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
                Assert.assertTrue(TextUtils.isEmpty(consumer.getTaData().getAccountId()));
            }
        };
        SDKTest test = new SDKTest();
        initUserIDCase("", "distinctId", "eventName", test);
        test.addVerify(onlyDistinctIdVerity);
        test.run();
        test.reset();
    }

    @Test
    public void testDistinctIdAndAccountId() {
        ISDKVerify accountIdAndDistinctIdVerity = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                Assert.assertNull(e);
                Assert.assertEquals("accountId", consumer.getTaData().getAccountId());
                Assert.assertEquals("distinctId", consumer.getTaData().getDistinctId());
            }
        };
        SDKTest test = new SDKTest();
        initUserIDCase("accountId", "distinctId", "eventName", test);
        test.addVerify(accountIdAndDistinctIdVerity);
        test.run();
        test.reset();
    }

    @Test
    public void testEmptyEventName() {
        ISDKVerify eventNameVerify = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                if (isStrictModel) {
                    Assert.assertNotNull(e);
                    Assert.assertEquals("The event name must be provided.", e.getMessage());
                }

            }
        };
        SDKTest test = new SDKTest();
        setUpEventCase("accountId", "", "", new HashMap<String, Object>(), test);
        test.addVerify(eventNameVerify);
        test.run();
        test.reset();
    }

    @Test
    public void testEventName() {
        ISDKVerify eventNameVerify = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                Assert.assertNull(e);
                Assert.assertEquals("eventName", consumer.getTaData().getEventName());
            }
        };
        SDKTest test = new SDKTest();
        setUpEventCase("accountId", "", "eventName", new HashMap<String, Object>(), test);
        test.addVerify(eventNameVerify);
        test.run();
        test.reset();
    }

    @Test
    public void testEmptyEventId() {
        ISDKVerify eventIdVerify = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                if (isStrictModel) {
                    Assert.assertNotNull(e);
                    Assert.assertEquals("The event id must be provided.", e.getMessage());
                }
            }
        };
        SDKTest test = new SDKTest();
        test.addCase(new ISDKCase<Exception>() {
            @Override
            public Exception doAction() {
                try {
                    sdk.track_update("accountId", "", "eventName", "", new HashMap<String, Object>());
                } catch (InvalidArgumentException e) {
                    return e;
                }
                return null;
            }
        });
        test.addCase(new ISDKCase<Exception>() {
            @Override
            public Exception doAction() {
                try {
                    sdk.track_overwrite("accountId", "", "eventName", "", new HashMap<String, Object>());
                } catch (InvalidArgumentException e) {
                    return e;
                }
                return null;
            }
        });
        test.addVerify(eventIdVerify);
        test.run();
        test.reset();

        test.addCase(new ISDKCase<Exception>() {
            @Override
            public Exception doAction() {
                try {
                    sdk.track_first("accountId", "", "eventName", new HashMap<String, Object>());
                } catch (InvalidArgumentException e) {
                    return e;
                }
                return null;
            }
        });
        test.addVerify(new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                Assert.assertNotNull(e);
                Assert.assertEquals("#first_check_id key must set", e.getMessage());
            }
        });
        test.run();
    }

    @Test
    public void testEventId() {
        ISDKVerify eventIdVerify = new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                Assert.assertNull(e);
                Assert.assertEquals("eventId", consumer.getTaData().getEventId());
            }
        };
        SDKTest test = new SDKTest();
        test.addCase(new ISDKCase<Exception>() {
            @Override
            public Exception doAction() {
                try {
                    sdk.track_update("accountId", "", "eventName", "eventId", new HashMap<String, Object>());
                } catch (InvalidArgumentException e) {
                    return e;
                }
                return null;
            }
        });
        test.addCase(new ISDKCase<Exception>() {
            @Override
            public Exception doAction() {
                try {
                    sdk.track_overwrite("accountId", "", "eventName", "eventId", new HashMap<String, Object>());
                } catch (InvalidArgumentException e) {
                    return e;
                }
                return null;
            }
        });
        test.addVerify(eventIdVerify);
        test.run();
        test.reset();

        test.addCase(new ISDKCase<Exception>() {
            @Override
            public Exception doAction() {
                try {
                    sdk.track_first("accountId", "", "eventName", new HashMap<String, Object>() {
                        {
                            put("#first_check_id", "eventId");
                        }
                    });
                } catch (InvalidArgumentException e) {
                    return e;
                }
                return null;
            }
        });
        test.addVerify(new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                Assert.assertNull(e);
                Assert.assertEquals("eventId", consumer.getTaData().getFirstCheckId());
            }
        });
        test.run();
    }

    @Test
    public void testType() {
        ISDKVerify eventTypeVerify = new ISDKVerify<String>() {
            @Override
            public void verify(String result) {
                Assert.assertEquals(result, consumer.getTaData().getType());
            }
        };
        SDKTest test = new SDKTest();
        setUpEventCase("accountId", "", "eventName", new HashMap<String, Object>(), test, "track", "track_update",
                "track_overwrite", "track");
        setUpUserPropertyCase("accountId", "", new HashMap<String, Object>(), test, "user_set", "user_setOnce",
                "user_append", "user_unset", "user_add", "user_del", "user_uniq_append");
        test.addVerify(eventTypeVerify);
        test.run();
    }

    @Test
    public void testTimeProperty() {
        ISDKVerify timeVerify = new ISDKVerify<Date>() {
            @Override
            public void verify(Date result) {
                if (!consumer.getTaData().getType().equals("user_del")
                        && !consumer.getTaData().getType().equals("user_unset")) {
                    Assert.assertEquals(result.toString(), consumer.getTaData().getTime().toString());
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
                }
            }
        };
        final SDKTest test = new SDKTest();
        final Date date = new Date(1657624662000L);
        setUpEventCase("accountId", "", "eventName", new HashMap<String, Object>() {
            {
                put("#time", date);
            }
        }, test, date, date, date, date);
        setUpUserPropertyCase("accountId", "", new HashMap<String, Object>() {
            {
                put("#time", date);
            }
        }, test, date, date, date, date, date, date, date);
        test.addVerify(timeVerify);
        test.run();
    }

    @Test
    public void testPresetProperty() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");
        final Date date = new Date();
        properties.put("#time", date);
        final String uuid = UUID.randomUUID().toString();
        properties.put("#uuid", uuid);
        final String ip = "192.168.10.10";
        properties.put("#ip", ip);
        final String appId = "testAppid";
        properties.put("#app_id", appId);
        final String firstCheckId = "firstCheckId";
        properties.put("#first_check_id", firstCheckId);
        final String transactionProperty = "transactionProperty";
        properties.put("#transaction_property", transactionProperty);
        final String importToolId = "importToolId";
        properties.put("#import_tool_id", importToolId);

        ISDKVerify timeVerify = new ISDKVerify<Date>() {
            @Override
            public void verify(Date result) {
                if (!consumer.getTaData().getType().equals("user_del")
                        && !consumer.getTaData().getType().equals("user_unset")) {
                    if (!consumer.getTaData().getType().equals("user_add")) {
                        Assert.assertEquals("male", consumer.getTaData().getPropertyObj().get("sex"));
                    }
                    Assert.assertEquals(date, consumer.getTaData().getTime());
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#time"));
                    Assert.assertEquals(uuid, consumer.getTaData().getUuid());
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#uuid"));
                    Assert.assertEquals(ip, consumer.getTaData().getIp());
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#ip"));
                    Assert.assertEquals(appId, consumer.getTaData().getAppId());
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#app_id"));
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#firstCheckId"));
                    Assert.assertEquals(transactionProperty, consumer.getTaData().getTransactionProperty());
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#transaction_property"));
                    Assert.assertEquals(importToolId, consumer.getTaData().getImportToolId());
                    Assert.assertNull(consumer.getTaData().getPropertyObj().get("#import_tool_id"));
                    Assert.assertEquals(23, consumer.getTaData().getPropertyObj().get("age"));
                }
                if (consumer.getTaData().getFirstCheckId() != null) {
                    Assert.assertEquals(firstCheckId, consumer.getTaData().getFirstCheckId());
                }
            }
        };
        final SDKTest test = new SDKTest();
        setUpEventCase("accountId", "", "eventName", properties, test, date, date, date, date);
        setUpUserPropertyCase("accountId", "", properties, test, date, date, date, date, date, date, date);
        test.addVerify(timeVerify);
        test.run();
    }

    @Test
    public void testUserAdd() {
        final SDKTest test = new SDKTest();
        this.<Exception>setUpUserAdd("accountId", "", new HashMap<String, Object>() {
            {
                put("a", "b");
            }
        }, test, null);
        test.addVerify(new ISDKVerify<Exception>() {
            @Override
            public void verify(Exception e) {
                if (isStrictModel) {
                    Assert.assertNotNull(e);
                    Assert.assertEquals("Only Number is allowed,Invalid property value b", e.getMessage());
                }
            }
        });
        test.run();
    }
    @Test
    public void testUUID() {
        init();
        // not open #uuid, and don't input #uuid. final data can't incloud #uuid
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");
        Exception expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        Map<String, Object> appendProperties = new HashMap<>();
        appendProperties.put("list", Arrays.asList("a", "b", "c"));
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        expect = null;
        Map<String, Object> addProperties = new HashMap<>();
        addProperties.put("age", 1);
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getUuid());

        String uuid = UUID.randomUUID().toString();
        properties.put("#uuid", uuid);
        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        System.out.println("uuid="+consumer.getTaData().getUuid());
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        appendProperties.put("#uuid", uuid);
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        expect = null;
        addProperties.put("#uuid", uuid);
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals(uuid, consumer.getTaData().getUuid());

        properties.remove("#uuid");
        sdk = new ThinkingDataAnalytics(consumer, true);
        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_set("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        try {
            sdk.user_setOnce("accountId", "distinctId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        appendProperties.remove("#uuid");
        try {
            sdk.user_append("accountId", "distinctId", appendProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());

        expect = null;
        addProperties.remove("#uuid");
        try {
            sdk.user_add("accountId", "distinctId", addProperties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNotNull(consumer.getTaData().getUuid());
    }

    @Test
    public void testPublicProperties() {
        init();
        Map<String, Object> superProperties = new HashMap<>();
        superProperties.put("test", "public");
        sdk.setSuperProperties(superProperties);
        // move #uuid, #ip, #time, #app_id to out
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 23);
        properties.put("sex", "male");

        Exception expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("public", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("public", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("public", consumer.getTaData().getPropertyObj().get("test"));

        properties.put("test", "private");

        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("private", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("private", consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertEquals("private", consumer.getTaData().getPropertyObj().get("test"));

        sdk.clearSuperProperties();
        properties.remove("test");

        expect = null;
        try {
            sdk.track("accountId", "distinctId", "eventName", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_update("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("test"));

        expect = null;
        try {
            sdk.track_overwrite("accountId", "distinctId", "eventName", "eventId", properties);
        } catch (Exception e) {
            expect = e;
        }
        Assert.assertNull(expect);
        Assert.assertNull(consumer.getTaData().getPropertyObj().get("test"));
    }
}