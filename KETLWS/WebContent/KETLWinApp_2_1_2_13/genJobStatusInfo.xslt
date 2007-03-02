<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="html" version="4.0" encoding="iso-8859-1" indent="yes"/>

	<xsl:template match="/">
		<xsl:apply-templates select="ETL"/>
	</xsl:template>

	<xsl:template match="ETL">
		<xsl:text disable-output-escaping="yes">
	&lt;!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd"&gt;
	&lt;%@ page contentType="text/html;charset=windows-1252"%&gt;
	&lt;%@ page import="java.util.*, java.text.*, java.web.*, mypackage.*" %&gt;
	</xsl:text>

		<html>
			<head>
				<meta name="vs_defaultClientScript" content="JavaScript"/>
				<STYLE TYPE="text/css">
					<xsl:text disable-output-escaping="yes">
&lt;!--
.pageBody
{    FONT-FAMILY: Arial, Helvetica, sans-serif;
	TEXT-ALIGN: left;
    BACKGROUND-COLOR: #ffffff
}
.columnHeadingSubOpen
{
	padding-top: 2px;
	padding-bottom: 2px;
	display: block;
	color: #3F3F3F;
	font-weight: bold;
	text-decoration: none;
	white-space: nowrap;
	font-size: 11px;
	TEXT-ALIGN: left;
    background: url("[APPLICATION_FULLPATH]/images/open_dna.gif") -2px no-repeat;
}
.columnHeadingSubClose
{
	padding-top: 2px;
	padding-bottom: 2px;
	display: block;
	color: #3F3F3F;
	font-weight: bold;
	text-decoration: none;
	white-space: nowrap;
	font-size: 11px;
	TEXT-ALIGN: left;
    background: url("[APPLICATION_FULLPATH]/images/closed_dna.gif") -2px no-repeat;
}
.outterTable
{
	border-left: 1px solid #996;
	border-right: 1px solid #996;
	border-bottom: 1px solid #996;
	border-top: 1px solid #996;	
	vertical-align: top;
	background: #eeefe1;	
}
.innerTable
{
	border-left: 1px solid #996;
	border-right: 1px solid #996;
	border-bottom: 1px solid #996;
	border-top: 1px solid #996;
    vertical-align: top;        
	background: #ffffff;		
}
.fieldName
{
    FONT-WEIGHT: bold;
    FONT-SIZE: 8pt;
    color: #3F3F3F;
    FONT-STYLE: normal;
    FONT-FAMILY: 'Arial';
    TEXT-ALIGN: right;
    padding-right: 2px;
    padding-left: 2px;
}
.fieldInputs
{
    FONT-SIZE: 8pt;
    color: #3F3F3F;	
    FONT-FAMILY: 'Arial';
    TEXT-ALIGN: left;
    padding-left: 4px;
    padding-right: 4px;
}
.columnName {
	padding: 4px 11px 3px 2px;
        text-indent: 2px;
	color: #3F3F3F;
	font-weight: bold;
	text-decoration: none;
	border-bottom: 1px solid #9a9964;
    text-align: left;
	font-size: 8pt;
	border-top: 1px solid #999999 ;
	border-left: 0;
	background: #D1D7B3;
}
.rowValue {
	font-size: 70%;
        text-indent: 2px;
        color: #3F3F3F;
	font-weight: normal;
    text-align: left;
	padding: 1px;
	padding-right: 4px;
	background: #fff;
    border-bottom: 1px  solid #AEBDD4;
}
.rowSeparator {
	border-bottom: 1px solid #996;
}
--&gt;
</xsl:text>
				</STYLE>
			</head>
			<body class="pageBody">

				<xsl:if test="child::node()[1]=LOAD">
					<xsl:apply-templates select="LOAD"/>
				</xsl:if>
				<xsl:if test="child::node()[1]=JOB">
					<table class="innerTable" width="100%" border="0" cellPadding="0" cellSpacing="0">
						<xsl:apply-templates select="JOB" mode="details"/>
					</table>
				</xsl:if>

			</body>
		</html>
		<script language="javascript">
    function ShowHide(src, section) {
            var ctrl = document.getElementById(section);
            if (ctrl.style.display == 'none') {
                    ctrl.style.display = 'block';
                    src.className = 'columnHeadingSubOpen';                    
            } else {
                    ctrl.style.display = 'none';
                    src.className = 'columnHeadingSubClose';
            }
    }
		</script>
	</xsl:template>


	<xsl:template match="LOAD">
		<xsl:variable name="seqLoad" select="position()" />
		<table class="outterTable" width="100%" border="0" cellPadding="0" cellSpacing="0">
			<tr>
				<td>
					<label class="fieldName">Load ID</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@LOAD_ID"/>
					</label>
					<label class="fieldName">Start Job ID</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@START_JOB_ID"/>
					</label>
					<label class="fieldName">Ignore Parents</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@IGNORED_PARENTS"/>
					</label>
					<label class="fieldName">Project ID</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@PROJECT_ID"/>
					</label>
					<br/>
					<label class="fieldName">Start Date</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@START_DATE"/>
					</label>
					<label class="fieldName">End Date</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@END_DATE"/>
					</label>
					<label class="fieldName">Is Running</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@RUNNING"/>
					</label>
					<label class="fieldName">Has Failed</label>
					<label class="fieldInputs">
						<xsl:value-of select="./@FAILED"/>
					</label>
				</td>
				<td width="12%" align="right">
					<xsl:element name="div">
						<xsl:attribute name="id">btnJobs<xsl:value-of select="$seqLoad"/>
						</xsl:attribute>
						<xsl:attribute name="class">columnHeadingSubClose</xsl:attribute>
						<xsl:attribute name="style">cursor:pointer;border-bottom:white</xsl:attribute>
						<xsl:attribute name="onclick">ShowHide(this, 'subJobs<xsl:value-of select="$seqLoad"/>')</xsl:attribute>
						<xsl:text disable-output-escaping="yes">&amp;nbsp; &amp;nbsp; &amp;nbsp; Job Details</xsl:text>
					</xsl:element>
				</td>
			</tr>
			<tr>
				<td style="font-size:2pt" colspan="2">
					<xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
				</td>
			</tr>
			<tr>
				<td style="padding-left:18px" colspan="2">
					<xsl:element name="table">
						<xsl:attribute name="id">subJobs<xsl:value-of select="$seqLoad"/>
						</xsl:attribute>
						<xsl:attribute name="class">innerTable</xsl:attribute>
						<xsl:attribute name="width">100%</xsl:attribute>
						<xsl:attribute name="border">0</xsl:attribute>
						<xsl:attribute name="cellPadding">0</xsl:attribute>
						<xsl:attribute name="cellSpacing">0</xsl:attribute>
						<xsl:attribute name="style">display:none</xsl:attribute>
						<xsl:apply-templates select="JOB" mode="details">
							<xsl:with-param name="seqLoad" select="$seqLoad"/>
						</xsl:apply-templates>
					</xsl:element>
				</td>
			</tr>
		</table>
	</xsl:template>

	<xsl:template match="JOB" mode="details">
		<xsl:param name="seqLoad"/>
		<xsl:variable name="seqJob" select="position()" />
		<tr>
			<td class="rowSeparator">
				<label class="fieldName">Execution ID</label>
				<label class="fieldInputs">
					<xsl:value-of select="./@EXECUTION_ID"/>
				</label>
				<label class="fieldName">Job ID</label>
				<label class="fieldInputs">
					<xsl:value-of select="./@ID"/>
				</label>
				<label class="fieldName">Server ID</label>
				<label class="fieldInputs">
					<xsl:value-of select="./@SERVER_ID"/>
				</label>
				<br/>
				<label class="fieldName">Start Date</label>
				<label class="fieldInputs">
					<xsl:value-of select="./@EXECUTION_DATE"/>
				</label>
				<label class="fieldName">End Date</label>
				<label class="fieldInputs">
					<xsl:value-of select="./@END_DATE"/>
				</label>
				<label class="fieldName">Status Text</label>
				<label class="fieldInputs">
					<xsl:value-of select="./@STATUS_TEXT"/>
				</label>
			</td>
			<td class="rowSeparator" width="12%" align="right">
				<xsl:if test="child::node()[1]=ERROR">
					<xsl:element name="div">
						<xsl:attribute name="id">btnErrors<xsl:value-of select="$seqLoad"/>-<xsl:value-of select="$seqJob"/>
						</xsl:attribute>
						<xsl:attribute name="class">columnHeadingSubClose</xsl:attribute>
						<xsl:attribute name="style">cursor:pointer;border-bottom:white</xsl:attribute>
						<xsl:attribute name="onclick">ShowHide(this, 'subErrors<xsl:value-of select="$seqLoad"/>-<xsl:value-of select="$seqJob"/>')</xsl:attribute>
						<xsl:text disable-output-escaping="yes">&amp;nbsp; &amp;nbsp; &amp;nbsp; Errors</xsl:text>
					</xsl:element>
				</xsl:if>
				<xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
			</td>
		</tr>
		<tr>
			<td style="font-size:2pt" colspan="2">
				<xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
			</td>
		</tr>
		<xsl:if test="child::node()[1]=ERROR">
			<tr>
				<td style="padding-left:18px" colspan="2">
					<xsl:element name="table">
						<xsl:attribute name="id">subErrors<xsl:value-of select="$seqLoad"/>-<xsl:value-of select="$seqJob"/>
						</xsl:attribute>
						<xsl:attribute name="class">innerTable</xsl:attribute>
						<xsl:attribute name="width">100%</xsl:attribute>
						<xsl:attribute name="border">0</xsl:attribute>
						<xsl:attribute name="cellPadding">0</xsl:attribute>
						<xsl:attribute name="cellSpacing">0</xsl:attribute>
						<xsl:attribute name="style">display:none</xsl:attribute>
						<tr>
							<th class="columnName">Code</th>
							<th class="columnName">Date-Time</th>
							<th class="columnName">Message</th>
							<th class="columnName">Details</th>
						</tr>
						<xsl:apply-templates select="ERROR" />
					</xsl:element>
				</td>
			</tr>
		</xsl:if>
	</xsl:template>

	<xsl:template match="ERROR">
		<tr>
			<td class="rowValue">
				<xsl:value-of select="./@CODE"/>
				<xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
			</td>
			<td class="rowValue" width="12%">
				<xsl:value-of select="./@DATETIME"/>
				<xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
			</td>
			<td class="rowValue">
				<xsl:value-of select="./@MESSAGE"/>
				<xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
			</td>
			<td class="rowValue" width="40%">
				<xsl:value-of select="./@DETAILS"/>
				<xsl:text disable-output-escaping="yes">&amp;nbsp;</xsl:text>
			</td>
		</tr>
	</xsl:template>

</xsl:stylesheet>
