﻿// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.Generic;
    using System.Web.Script.Serialization;
    using NUnit.Framework;

    using GridGain.Client.Ssl;

    [TestFixture]
    public class GridClientRouterHttpTest : GridClientHttpTest {
        override protected String RouterAddress() {
            return Host + ":" + RouterHttpPort;
        }
    }
}
