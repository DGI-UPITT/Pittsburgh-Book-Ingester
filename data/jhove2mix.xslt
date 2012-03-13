<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:mix="http://www.loc.gov/mix/v20" version="1.0" exclude-result-prefixes="mix">
    <xsl:template match="/">
        <xsl:apply-templates select="//mix:mix"/>
    </xsl:template>
    <xsl:template match="mix:mix">
        <xsl:copy-of select="."/>
    </xsl:template>
</xsl:stylesheet>
