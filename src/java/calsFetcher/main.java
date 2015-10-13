package calsFetcher;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import cern.accsoft.cals.extr.client.service.MetaDataService;
import cern.accsoft.cals.extr.client.service.ServiceBuilder;
import cern.accsoft.cals.extr.client.service.TimeseriesDataService;
import cern.accsoft.cals.extr.domain.core.constants.VariableDataType;
import cern.accsoft.cals.extr.domain.core.datasource.DataLocationPreferences;
import cern.accsoft.cals.extr.domain.core.metadata.Hierarchy;
import cern.accsoft.cals.extr.domain.core.metadata.Variable;
import cern.accsoft.cals.extr.domain.core.metadata.VariableSet;
import cern.accsoft.cals.extr.domain.core.timeseriesdata.TimeseriesData;
import cern.accsoft.cals.extr.domain.core.timeseriesdata.TimeseriesDataSet;
import cern.accsoft.cals.extr.domain.exceptions.DataAccessException;
import cern.accsoft.cals.extr.domain.util.TimestampFactory;

public class main {

    // definizione variabili applicazione

    private MetaDataService metaDataService;
    private TimeseriesDataService timeseriesDataService;
    private static final String CLIENT_NAME = "MP3";
    private static final String APP_NAME = "MP3DE";
    /*
     * RPTE.UA83.RB.A78:I_EARTH_PCNT RPTE.UA83.RB.A78:I_MEAS RQD.A78 on 24/7/2015
     */
    public String nodePath = "LHC.Power Converters.Powering SubSectors.A12.I_DIFF_MA";
    public static final String[] variableArray = { "RPTE.UA83.RB.A78:I_EARTH_PCNT" };
    // public static final String[] variableArray = {"%"};
    // public static final String[] variableArray = {"RB.A78:I_EARTH_PCNT"};

    // UTC or LOCALTIME ( parseUTCTime <-> parseLocalTime)
    private static final Timestamp startTime = TimestampFactory.parseLocalTimestamp("2015-08-18 10:00:00");
    private static final Timestamp endTime = TimestampFactory.parseLocalTimestamp("2015-08-18 12:01:00");

    private VariableSet variables = null;
    private List<String> Names;
    // private SortedSet<Hierarchy> allHierarchies; // iterator usable. (not on HierarchySet)
    public Hierarchy LVL1;
    public Hierarchy LVL2;
    public Hierarchy LVL3;
    public Hierarchy LVL4;

    private void initControllers() {
        this.metaDataService = ServiceBuilder.getInstance(APP_NAME, CLIENT_NAME,
                DataLocationPreferences.MDB_AND_LDB_PRO).createMetaService();

        this.timeseriesDataService = ServiceBuilder.getInstance(APP_NAME, CLIENT_NAME,
                DataLocationPreferences.MDB_AND_LDB_PRO).createTimeseriesService();
    }

    public void GetTimeSeriesDemo() {
        initControllers();
        setVariables();
    }

    public Hierarchy fetchHierarchyByName(SortedSet<Hierarchy> allHierarchies, String name) {
        Iterator<Hierarchy> I = allHierarchies.iterator();
        while (I.hasNext()) {
            Hierarchy Htemp = I.next();
            String nameH = Htemp.getHierarchyName();
            if (nameH.equals(name))
                return Htemp;
        }
        return null;
    }

    public void pathFinder() {
        String step0 = "LHC";
        String step1 = "Power Converters";
        String step2 = "163";
        String step3 = "PHALL";

        this.LVL1 = fetchHierarchyByName(this.metaDataService.getTopLevelHierarchies().getAllHierachies(), step0);
        this.LVL2 = fetchHierarchyByName(this.metaDataService.getHierarchyChildNodes(this.LVL1).getAllHierachies(),
                step1);
        this.LVL3 = fetchHierarchyByName(this.metaDataService.getHierarchyChildNodes(this.LVL2).getAllHierachies(),
                step2);
        this.LVL4 = fetchHierarchyByName(this.metaDataService.getHierarchyChildNodes(this.LVL3).getAllHierachies(),
                step3);
        prova();
    }

    public void prova() {
        VariableDataType a = VariableDataType.ALL;
        this.variables = this.metaDataService.getVariablesOfDataTypeAttachedToHierarchy(LVL4, a);
        Iterator<Variable> variablesIt = this.variables.iterator();
        while (variablesIt.hasNext()) {
            Variable variable = variablesIt.next();
            System.out.println(variable.getVariableName());
        }
    }

    public void exploreLVL(Hierarchy X) {
        SortedSet<Hierarchy> SS = this.metaDataService.getHierarchyChildNodes(X).getAllHierachies();
        System.out.println(this.metaDataService.getHierarchyChildNodes(X).getAllHierachies().size());
        Iterator<Hierarchy> I = SS.iterator();
        while (I.hasNext()) {
            Hierarchy Htemp = I.next();
            String nameH = Htemp.getHierarchyName();
            System.out.println("NOME: " + nameH);
            System.out.println("        DESCRIZIONE: " + Htemp.getDescription());
        }
    }

    public void printTimeseriesDataSetFile() throws NoSuchMethodException {
        Iterator<Variable> variablesIt = this.variables.iterator();
        while (variablesIt.hasNext()) {
            Variable variable = variablesIt.next();
            TimeseriesDataSet values = this.timeseriesDataService.getDataInTimeWindow(variable, startTime, endTime);
            int size = values.size();
            System.out.println("Values for variable : " + variable.getVariableName() + " size: " + size);
            for (int i = 0; i < size; i++) {
                TimeseriesData dataPoint = values.getTimeSeriesData(i);
                System.out.println(dataPoint.getStamp() + ": " + dataPoint.getDoubleValue());
            }
        }
    }

    private void setVariables() {
        try {
            this.variables = this.metaDataService.getVariablesWithNameInListofStrings(Arrays.asList(variableArray));
        } catch (DataAccessException daEx) {
            daEx.printStackTrace();
        }
    }

    public static void main(String args[]) {
        main exportDemo = new main();
        exportDemo.GetTimeSeriesDemo();
        try {
            exportDemo.pathFinder();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("END");
    }
}
