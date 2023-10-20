package cn.thinkingdata.analytics.test.functionTest;

import cn.thinkingdata.analytics.util.TDPropertyUtil;
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
        Assert.assertFalse( TDPropertyUtil.isValidKey(null));
        Assert.assertFalse(TDPropertyUtil.isValidKey(""));
        Assert.assertFalse(TDPropertyUtil.isValidKey("_"));
        Assert.assertTrue(TDPropertyUtil.isValidKey("#a"));
        Assert.assertTrue(TDPropertyUtil.isValidKey("a"));
    }
}
