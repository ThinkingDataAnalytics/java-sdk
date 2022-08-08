package cn.thinkingdata.tga.javasdk.test;

import cn.thinkingdata.tga.javasdk.util.TAPropertyUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UtilTest {
    @Before
    public void init()
    {
    }
    @Test
    //验证Properties key的匹配规则
    public void test1()
    {
        Assert.assertFalse( TAPropertyUtil.isValidKey(null));
        Assert.assertFalse(TAPropertyUtil.isValidKey(""));
        Assert.assertFalse(TAPropertyUtil.isValidKey("_"));
        Assert.assertTrue(TAPropertyUtil.isValidKey("#a"));
        Assert.assertTrue(TAPropertyUtil.isValidKey("a"));
    }
}
