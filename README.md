# PDF Extractor Transform


Description
-----------
This transform leverages the [Apache PDF Box](https://pdfbox.apache.org/) library to extract text and metadata from a PDF file. It is usually used in conjunction with the Whole File Reader plugin since it requires the entire contents of the PDF to be loaded into a single message and passed into the transform. Due to this, there may be memory issues when loading extremely large PDF files. 

Use Case
--------
Say I am given a large directory full of PDF files, possibly a set of papers or legal documents, and I need to quickly extract the title, author, and text from these documents. I can read the files using the Whole File Reader plugin, then leverage this transform to extract the required information, before writing the text to a table for easy querying.

Properties
----------
| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Source Field Name** | **Y** | None | This is the name of the field on the input record containing the pdf file. It must be of type ``bytes`` and it must contain the entire contents of the PDF file. |
| **Continue Processing If There Are Errors?** | **Y** | false | Indicates if the pipeline should continue if processing a single PDF fails. |

Build
-----
To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

UI Integration
--------------
The CDAP UI displays each plugin property as a simple textbox. To customize how the plugin properties
are displayed in the UI, you can place a configuration file in the ``widgets`` directory.
The file must be named following a convention of ``[plugin-name]-[plugin-type].json``.

See [Plugin Widget Configuration](http://docs.cdap.io/cdap/current/en/hydrator-manual/developing-plugins/packaging-plugins.html#plugin-widget-json)
for details on the configuration file.

The UI will also display a reference doc for your plugin if you place a file in the ``docs`` directory
that follows the convention of ``[plugin-name]-[plugin-type].md``.

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json file under the ``target`` directory. This file is deployed along with your .jar file to add your
plugins to CDAP.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, here if your artifact is named 'azure-decompress-action-1.0.0.jar':

    > load artifact target/pdf-extractor-transform-1.0.0.jar config-file target/pdf-extractor-transform-1.0.0.json

## Mailing Lists

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
