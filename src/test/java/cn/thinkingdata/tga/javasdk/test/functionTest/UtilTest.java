package cn.thinkingdata.tga.javasdk.test.functionTest;

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
    // check rules of keys
    public void test1()
    {
        Assert.assertFalse( TAPropertyUtil.isValidKey(null));
        Assert.assertFalse(TAPropertyUtil.isValidKey(""));
        Assert.assertFalse(TAPropertyUtil.isValidKey("_"));
        Assert.assertTrue(TAPropertyUtil.isValidKey("#a"));
        Assert.assertTrue(TAPropertyUtil.isValidKey("a"));
    }
}
